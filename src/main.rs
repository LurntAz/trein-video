use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};
use trein_video::{api, cli, config, db, nas, startup, sync, video, worker};

/// Initialize the global `tracing` subscriber.
///
/// Format is controlled by `LOG_FORMAT` (`json` or `pretty`, default `pretty`).
/// Verbosity is controlled by `RUST_LOG`; if unset, defaults to
/// `trein_video=info,warn` so that noisy third-party crates (hyper, sqlx, ...)
/// stay quiet unless explicitly requested.
fn init_logging() {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("trein_video=info,warn"));

    let json_format = std::env::var("LOG_FORMAT")
        .map(|v| v.eq_ignore_ascii_case("json"))
        .unwrap_or(false);

    if json_format {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .with_current_span(true)
            .with_span_list(true)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .pretty()
            .init();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();

    let args = cli::parse_args();
    // Resolved before `load_config` consumes `args.config`, purely for
    // display in the master startup summary (#33) -- `load_config` already
    // does the equivalent resolution internally.
    let config_path = args.config.clone().or_else(cli::default_config_path);
    let config = config::load_config(args.config)?;

    info!(
        instance_id = %config.instance.id,
        role = %config.instance.role,
        "Starting trein-video"
    );

    // Fail fast at boot if the external binaries we shell out to are
    // missing, rather than discovering it in the middle of a job.
    if let Err(missing) = check_required_binaries().await {
        warn!(
            "Some required external binaries are missing: {missing:?}. \
             Related features will fail at runtime."
        );
    }

    match config.instance.role.as_str() {
        "master" => {
            info!("Starting as MASTER instance");
            run_master(config, config_path).await?;
        }
        "worker" => {
            info!("Starting as WORKER instance");
            run_worker(config).await?;
        }
        _ => {
            error!(role = %config.instance.role, "Invalid role");
            anyhow::bail!("Invalid role: {}", config.instance.role);
        }
    }

    Ok(())
}

/// Best-effort check for `ffmpeg`/`ffprobe`/`smbclient` on PATH. Returns the
/// list of missing binaries (does not fail startup, since a worker-only or
/// master-only instance may not need all of them).
async fn check_required_binaries() -> std::result::Result<(), Vec<&'static str>> {
    let candidates = ["ffmpeg", "ffprobe", "smbclient"];
    let mut missing = Vec::new();
    for bin in candidates {
        let found = tokio::process::Command::new(bin)
            .arg("-version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await
            .is_ok();
        if !found {
            missing.push(bin);
        }
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(missing)
    }
}

async fn run_master(config: config::Config, config_path: Option<PathBuf>) -> Result<()> {
    use api::discovery::ServiceDiscovery;
    use api::server::ApiServer;
    use api::video_discovery::VideoDiscovery;
    use db::{DbConnection, Repository};
    use nas::SmbClient;
    use startup::summary::{count_indexes, count_tables, redact_webhook};
    use startup::{run_preflight_checks, CheckStatus};

    info!(port = config.instance.api_port, "Master initializing");

    // --- #33: orchestrate #30 (config, already loaded)/#31 (DB schema)/
    // #32 (preflight) up front, before mDNS/video-discovery/the API server
    // -- there's no point publishing this instance or scanning the NAS on
    // a schedule if the DB itself is broken. `DbConnection::new` already
    // runs migrations, `verify_db_schema` and `create_optimized_indexes`
    // (#31) internally.
    let db_connection = DbConnection::new(&config.db.path).await?;
    let repository = Arc::new(Repository::new(db_connection.pool().clone()));

    let checks = run_preflight_checks(&config, &db_connection).await;
    let db_check = checks
        .iter()
        .find(|c| c.name == "Database")
        .expect("run_preflight_checks always includes a Database check");
    let nas_check = checks
        .iter()
        .find(|c| c.name == "NAS")
        .expect("run_preflight_checks always includes a NAS check");
    let discord_check = checks
        .iter()
        .find(|c| c.name == "Discord")
        .expect("run_preflight_checks always includes a Discord check");

    let tables = count_tables(db_connection.pool()).await.unwrap_or(0);
    let indexes = count_indexes(db_connection.pool()).await.unwrap_or(0);

    // Log password status for debugging (kept from the pre-#33 video
    // discovery setup below, computed once here since the same `SmbClient`
    // is now shared between the preflight file-count probe and video
    // discovery).
    let pwd = config.nas.get_password();
    if pwd.is_some() {
        info!("NAS: password loaded successfully");
    } else {
        warn!("NAS: PASSWORD NOT FOUND! Check NAS_PASSWORD env var");
    }
    let smb_client = Arc::new(SmbClient::new(
        config.nas.host.clone(),
        config.nas.share.clone(),
        config.nas.username.clone(),
        pwd.clone(),
    ));

    // Best-effort file listing purely to populate the summary's "listed N
    // files" figure -- `nas_check` above already determined reachability
    // (pass/fail); this never changes that outcome, only decorates it.
    let file_count = if nas_check.status != CheckStatus::Error {
        smb_client
            .list_videos(&config.nas.base_path)
            .await
            .ok()
            .map(|entries| entries.len())
    } else {
        None
    };

    let summary = startup::StartupSummary {
        instance_id: config.instance.id.clone(),
        config_path: config_path.unwrap_or_else(|| PathBuf::from("<unresolved>")),
        codec: config.conversion.codec.clone(),
        preset: config.conversion.preset.clone(),
        max_parallel_jobs: config.conversion.max_parallel_jobs,
        db: startup::DbInfo {
            tables,
            indexes,
            // See `SqlitePoolOptions::max_connections` in
            // `db::connection::DbConnection::new` -- not exposed by `sqlx`
            // at runtime, so mirrored here as the one place both need it.
            pool_size: 5,
            status: db_check.status,
            message: db_check.message.clone(),
        },
        nas: startup::NasInfo {
            host: config.nas.host.clone(),
            share: config.nas.share.clone(),
            base_path: config.nas.base_path.clone(),
            file_count,
            status: nas_check.status,
            message: nas_check.message.clone(),
        },
        discord: startup::DiscordInfo {
            enabled: config.discord.enabled,
            webhook_redacted: config
                .discord
                .enabled
                .then(|| redact_webhook(&config.discord.webhook_url)),
            status: discord_check.status,
            message: discord_check.message.clone(),
        },
        api: startup::ApiInfo {
            bind_addr: "0.0.0.0".to_string(),
            port: config.instance.api_port,
        },
    };

    info!(
        instance_id = %summary.instance_id,
        config_path = %summary.config_path.display(),
        codec = %summary.codec,
        preset = %summary.preset,
        max_parallel_jobs = summary.max_parallel_jobs,
        db_status = ?summary.db.status,
        db_tables = summary.db.tables,
        db_indexes = summary.db.indexes,
        nas_status = ?summary.nas.status,
        nas_file_count = ?summary.nas.file_count,
        discord_enabled = summary.discord.enabled,
        discord_status = ?summary.discord.status,
        api_port = summary.api.port,
        "Master startup summary"
    );

    // The boxed summary itself is a human-facing terminal artifact,
    // deliberately printed to stdout rather than through `tracing` (which
    // the `info!` call above already covers for logs/aggregation).
    println!("{}", summary.render());

    if summary.has_critical_failure() {
        error!(
            db_status = ?summary.db.status,
            nas_status = ?summary.nas.status,
            "Critical preflight failure (DB and/or NAS); aborting master startup"
        );
        anyhow::bail!(
            "master startup aborted: critical preflight check failed (db: {:?}, nas: {:?}); \
             see the startup summary above for details",
            summary.db.status,
            summary.nas.status
        );
    }
    // --- end #33 orchestration -------------------------------------------

    let mut discovery_handle = None;
    if config.discovery.enabled {
        info!("Setting up mDNS discovery");
        let daemon = ServiceDiscovery::publish_master_service(
            &config.discovery.service_name,
            &config.instance.id,
            config.instance.api_port,
        )?;
        discovery_handle = Some(daemon);
        info!("mDNS discovery setup complete");
    }

    // Video discovery: scan NAS folder for videos
    if config.video_discovery.enabled {
        info!("Setting up video discovery");

        let video_discovery = Arc::new(VideoDiscovery::new(
            smb_client,
            repository,
            config.nas.base_path.clone(),
        ));
        let interval = Duration::from_secs(config.video_discovery.interval_secs);
        tokio::spawn(run_video_discovery_loop(video_discovery, interval));
        info!(
            "Video discovery setup complete, running every {} secs",
            config.video_discovery.interval_secs
        );
    }

    info!("Creating API server");
    let server = ApiServer::new(config.clone());
    info!("Starting API server");
    server.start().await?;

    // Keep the mDNS daemon alive for as long as the server runs.
    drop(discovery_handle);

    Ok(())
}

async fn run_video_discovery_loop(
    discovery: Arc<api::video_discovery::VideoDiscovery>,
    interval: Duration,
) {
    loop {
        match discovery.discover_videos().await {
            Ok(count) => {
                if count > 0 {
                    info!("Video discovery found {} new videos", count);
                }
            }
            Err(e) => {
                warn!("Video discovery failed: {}", e);
            }
        }
        tokio::time::sleep(interval).await;
    }
}

async fn run_worker(config: config::Config) -> Result<()> {
    use api::discovery::DEFAULT_DISCOVERY_TIMEOUT;
    use db::{DbConnection, Repository};
    use nas::SmbClient;
    use std::sync::Arc;
    use std::time::Duration;
    use sync::{Coordinator, HttpMasterClient};
    use trein_video::discord::DiscordNotifier;
    use trein_video::progress::{self, ProgressDisplay};
    use video::VideoConverter;
    use worker::{JobQueue, PipelineRunner};

    info!(worker_id = %config.instance.id, "Worker starting main processing loop");

    // #20: real-time progress display. `tx` is handed to the pipeline
    // (`PipelineRunner` below); `rx` is owned by the display task, which
    // renders events as they arrive (live progress bars on a TTY, plain
    // status lines otherwise) for as long as the worker runs.
    let (progress_tx, progress_rx) = progress::channel();
    tokio::spawn(ProgressDisplay::new(progress_rx).run());

    let db_connection = DbConnection::new(&config.db.path).await?;
    let repository = Arc::new(Repository::new(db_connection.pool().clone()));

    let smb_client = Arc::new(SmbClient::new(
        config.nas.host.clone(),
        config.nas.share.clone(),
        config.nas.username.clone(),
        config.nas.password.clone(),
    ));
    let converter = Arc::new(VideoConverter::new(
        config.conversion.preset.clone(),
        config.conversion.crf,
    ));

    let work_dir =
        std::path::PathBuf::from(std::env::var("HOME").unwrap_or_else(|_| ".".to_string()))
            .join(".cache")
            .join("trein-video")
            .join("work");

    // Discord webhook notifications (optional): the worker posts a message
    // when a conversion job finishes, success or failure. `config.discord`
    // defaults to disabled, so most configs simply run without this --
    // `PipelineRunner`/`ProcessorOrchestrator` skip sending anything when
    // `discord_notifier` is `None`. Only workers send notifications (the
    // master doesn't process videos, so it has nothing to report).
    let discord_notifier = if config.discord.enabled {
        // Never log the webhook URL itself -- it's a bearer credential (
        // anyone with it can post to the channel), same sensitivity class
        // as `config.nas.password`.
        info!("Discord notifications enabled");
        Some(Arc::new(DiscordNotifier::new(
            config.discord.webhook_url.clone(),
        )))
    } else {
        info!("Discord notifications disabled");
        None
    };

    let runner = Arc::new(PipelineRunner {
        smb_client,
        converter,
        repository: repository.clone(),
        work_dir,
        conversion_config: config.conversion.clone(),
        retry_config: config.retry.clone(),
        progress_tx,
        discord_notifier,
    });

    let queue = JobQueue::new(
        repository.clone(),
        runner,
        config.instance.id.clone(),
        config.conversion.max_parallel_jobs,
    );

    // #15: resolve the master's URL -- either explicitly configured, or
    // discovered via mDNS (#13) -- and spawn the background sync/heartbeat
    // loop alongside the queue's own processing loop. A master that's
    // temporarily unreachable (during discovery or later, once running)
    // must never prevent this worker from processing videos it already has
    // locally, so any failure here is logged and degrades to "no sync" for
    // this instance rather than aborting startup.
    match resolve_master_url(&config, DEFAULT_DISCOVERY_TIMEOUT).await {
        Ok(master_url) => {
            info!(%master_url, "worker sync coordinator connecting to master");
            match HttpMasterClient::new(master_url.clone(), &config.tls) {
                Ok(client) => {
                    info!("worker - HttpMasterClient created successfully");
                    // Workers don't run their own API server (only the master
                    // does, see `run_master`), so this is informational only --
                    // nothing currently dials back into it.
                    let self_api_url = format!(
                        "https://{}:{}",
                        config.instance.id, config.instance.api_port
                    );
                    let coordinator = Arc::new(Coordinator::new(
                        Arc::new(client),
                        repository.clone(),
                        config.instance.id.clone(),
                        self_api_url,
                    ));
                    info!("worker - spawning coordinator run_loop");
                    tokio::spawn(
                        coordinator.run_loop(Duration::from_secs(config.sync.poll_interval_secs)),
                    );
                    info!("worker - coordinator spawned");
                }
                Err(e) => {
                    warn!(error = %e, "failed to build mTLS client for master sync; running without sync");
                }
            }
        }
        Err(e) => {
            warn!(error = %e, "could not resolve master URL; running without sync");
        }
    }

    info!("worker - starting job queue processing");
    queue.process().await?;
    info!("worker - job queue processing ended");
    Ok(())
}

/// `sync.master_url` if configured, otherwise discover it via mDNS (#13).
/// `config::validate_config` already rejects a worker config with neither
/// `sync.master_url` set nor discovery implied, but discovery can still
/// legitimately time out at runtime (master not up yet, network hiccup).
async fn resolve_master_url(
    config: &config::Config,
    discovery_timeout: std::time::Duration,
) -> Result<String> {
    if let Some(url) = &config.sync.master_url {
        return Ok(url.clone());
    }

    use api::discovery::ServiceDiscovery;
    let (host, port) = ServiceDiscovery::discover_master_service(discovery_timeout).await?;
    Ok(format!("https://{host}:{port}"))
}
