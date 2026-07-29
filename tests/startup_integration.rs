//! End-to-end integration tests for the master startup sequence (#35):
//! config loading (#30) -> DB init + schema verification (#31) -> preflight
//! checks (#32) -> startup summary (#33).
//!
//! Driven entirely through `trein_video`'s public API -- the actual
//! orchestration glue in `src/main.rs::run_master` is a private binary-only
//! function (and also stands up mDNS/the real API server), so the "full
//! sequence" tests below re-assemble the same four steps by hand, exactly
//! the way `run_master` itself does, and assert on the result.
//!
//! No real NAS/SMB server is available in this test environment, so the NAS
//! preflight check is exercised for its *unreachable* path only (see
//! `test_preflight_nas_unreachable_error` and the comment on
//! `test_master_startup_sequence_success`) -- this mirrors the precedent
//! already set by `src/startup/preflight.rs`'s own unit tests, which use the
//! same `127.0.0.1` non-listening-host trick.
//!
//! Every test gets its own `tempfile::tempdir()` (config file + SQLite DB
//! all live under it) so tests can run in parallel (cargo test's default)
//! without interfering with each other, matching the convention already
//! used in `tests/api_integration.rs` / `tests/pipeline_integration.rs`.

use std::path::{Path, PathBuf};

use trein_video::cli;
use trein_video::config::{
    self, Config, ConversionConfig, DbConfig, DiscordConfig, DiscoveryConfig, InstanceConfig,
    NasConfig, RetryConfig, SyncConfig, TlsConfig, VideoDiscoveryConfig,
};
use trein_video::db::{create_optimized_indexes, verify_db_schema, DbConnection};
use trein_video::startup::summary::{count_indexes, count_tables, redact_webhook};
use trein_video::startup::{
    run_preflight_checks, ApiInfo, CheckStatus, DbInfo, DiscordInfo, NasInfo, StartupSummary,
};

mod common {
    use super::*;

    fn path_str(p: &Path) -> String {
        p.to_string_lossy().to_string()
    }

    /// A minimal, fully-valid **master** config TOML, with the DB at
    /// `db_path`. NAS host is `127.0.0.1` -- nothing listens there in this
    /// test environment, which is deliberate: tests that only care about
    /// config parsing / DB setup don't need a real SMB server, and tests
    /// that specifically exercise the NAS preflight check rely on that same
    /// unreachability to produce a deterministic failure.
    pub fn master_toml(db_path: &Path) -> String {
        format!(
            r#"
[instance]
id = "test-master"
role = "master"
api_port = 8000

[nas]
protocol = "smb"
host = "127.0.0.1"
share = "videos"
username = "user"
password = "pw"
base_path = "/videos"

[conversion]
codec = "av1"
preset = "slow"
crf = 32
max_parallel_jobs = 2

[sync]
poll_interval_secs = 30

[db]
path = "{db}"

[tls]
cert_path = "{db}.cert"
key_path = "{db}.key"
ca_cert_path = "{db}.ca"

[discovery]
enabled = false
service_name = "test-svc"
"#,
            db = path_str(db_path)
        )
    }

    /// Same shape as [`master_toml`], but `role = "worker"` with no
    /// `sync.master_url` set -- `load_config`'s validation must reject this
    /// (a worker with no way to find its master).
    pub fn worker_toml_missing_master_url(db_path: &Path) -> String {
        master_toml(db_path).replacen(r#"role = "master""#, r#"role = "worker""#, 1)
    }

    /// [`master_toml`] with a `~/...` path for `[db] path`, to exercise
    /// `load_config`'s tilde expansion. `suffix` is relative to `$HOME`.
    pub fn master_toml_with_tilde_db_path(suffix: &str) -> String {
        let tilde_path = format!("~/{suffix}");
        master_toml(Path::new(&tilde_path))
    }

    pub fn write_config(dir: &Path, contents: &str) -> PathBuf {
        let path = dir.join("config.toml");
        std::fs::write(&path, contents).unwrap();
        path
    }

    /// A ready-to-use master [`Config`] built directly (no TOML round-trip)
    /// for tests that need to tweak individual fields -- same pattern as
    /// `tests/pipeline_integration.rs::master_config`.
    pub fn build_config(dir: &Path) -> Config {
        Config {
            instance: InstanceConfig {
                id: "test-master".to_string(),
                role: "master".to_string(),
                api_port: 0,
            },
            nas: NasConfig {
                protocol: "smb".to_string(),
                host: "127.0.0.1".to_string(),
                share: "videos".to_string(),
                username: "user".to_string(),
                password: Some("pw".to_string()),
                password_env: None,
                base_path: "/videos".to_string(),
            },
            conversion: ConversionConfig {
                codec: "av1".to_string(),
                preset: "slow".to_string(),
                crf: 32,
                max_parallel_jobs: 2,
            },
            sync: SyncConfig {
                poll_interval_secs: 30,
                master_url: None,
            },
            db: DbConfig {
                path: path_str(&dir.join("test.db")),
            },
            tls: TlsConfig {
                cert_path: path_str(&dir.join("certs/server.crt")),
                key_path: path_str(&dir.join("certs/server.key")),
                ca_cert_path: path_str(&dir.join("certs/ca.crt")),
            },
            discovery: DiscoveryConfig {
                enabled: false,
                service_name: "test-svc".to_string(),
            },
            video_discovery: VideoDiscoveryConfig::default(),
            retry: RetryConfig::default(),
            discord: DiscordConfig::default(),
        }
    }

    pub fn ok_db_info() -> DbInfo {
        DbInfo {
            tables: 4,
            indexes: 5,
            pool_size: 5,
            status: CheckStatus::Ok,
            message: "connection OK".to_string(),
        }
    }

    pub fn ok_nas_info() -> NasInfo {
        NasInfo {
            host: "192.168.1.100".to_string(),
            share: "videos".to_string(),
            base_path: "/movies".to_string(),
            file_count: Some(7),
            status: CheckStatus::Ok,
            message: "reachable".to_string(),
        }
    }

    pub fn disabled_discord_info() -> DiscordInfo {
        DiscordInfo {
            enabled: false,
            webhook_redacted: None,
            status: CheckStatus::Ok,
            message: "Disabled".to_string(),
        }
    }

    /// A fully-populated, valid [`StartupSummary`] for tests that only care
    /// about `render()`'s output, not how each section got built.
    pub fn sample_summary(instance_id: &str) -> StartupSummary {
        StartupSummary {
            instance_id: instance_id.to_string(),
            config_path: PathBuf::from("/home/user/.config/trein-video/config.toml"),
            codec: "av1".to_string(),
            preset: "slow".to_string(),
            max_parallel_jobs: 2,
            db: ok_db_info(),
            nas: ok_nas_info(),
            discord: disabled_discord_info(),
            api: ApiInfo {
                bind_addr: "0.0.0.0".to_string(),
                port: 8000,
            },
        }
    }
}

// ---------------------------------------------------------------------
// 1. Config loading and template generation (#30)
// ---------------------------------------------------------------------

#[tokio::test]
async fn test_config_missing_generates_template() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    assert!(!path.exists());

    let err = config::load_config(Some(path.clone())).unwrap_err();

    // Template was written to the resolved path...
    assert!(path.exists(), "a template must be written on first run");
    let template = std::fs::read_to_string(&path).unwrap();
    assert!(template.contains("[instance]"));
    assert!(template.contains("[nas]"));
    assert!(template.contains("[discord]"));

    // ...and the call itself is an error (non-zero exit for the caller),
    // not a silent success with placeholder values.
    let message = format!("{err}").to_lowercase();
    assert!(
        message.contains("no config file"),
        "unexpected error message: {message}"
    );
}

#[tokio::test]
async fn test_config_existing_loaded_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("videos.db");
    let toml = common::master_toml(&db_path);
    let path = common::write_config(dir.path(), &toml);

    let cfg = config::load_config(Some(path)).unwrap();

    assert_eq!(cfg.instance.id, "test-master");
    assert_eq!(cfg.instance.role, "master");
    assert_eq!(cfg.instance.api_port, 8000);
    assert_eq!(cfg.nas.protocol, "smb");
    assert_eq!(cfg.nas.host, "127.0.0.1");
    assert_eq!(cfg.nas.share, "videos");
    assert_eq!(cfg.nas.base_path, "/videos");
    assert_eq!(cfg.conversion.codec, "av1");
    assert_eq!(cfg.conversion.preset, "slow");
    assert_eq!(cfg.conversion.crf, 32);
    assert_eq!(cfg.conversion.max_parallel_jobs, 2);
    assert_eq!(cfg.sync.poll_interval_secs, 30);
    assert_eq!(cfg.sync.master_url, None);
    assert_eq!(cfg.db.path, db_path.to_string_lossy());
    assert!(!cfg.discovery.enabled);
    // No [discord] section in the TOML => disabled by default (#30/#DiscordConfig).
    assert!(!cfg.discord.enabled);
    // No [retry] section => falls back to RetryConfig::default().
    assert_eq!(cfg.retry.max_attempts, 5);
}

#[tokio::test]
async fn test_config_invalid_toml_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    let garbage = "this [ is not ] valid toml {{{";
    std::fs::write(&path, garbage).unwrap();

    let err = config::load_config(Some(path.clone()));
    assert!(err.is_err());

    // An existing (even invalid) config must never be overwritten with the
    // first-run template.
    assert_eq!(std::fs::read_to_string(&path).unwrap(), garbage);
    let message = format!("{}", err.unwrap_err()).to_lowercase();
    assert!(
        !message.contains("no config file"),
        "an existing file must not be treated as first-run: {message}"
    );
}

// ---------------------------------------------------------------------
// 2. Database initialization and schema verification (#31)
// ---------------------------------------------------------------------

#[tokio::test]
async fn test_db_schema_verification_passes() {
    let dir = tempfile::tempdir().unwrap();
    let conn = DbConnection::new(dir.path().join("test.db")).await.unwrap();

    // `DbConnection::new` already calls `verify_db_schema` internally (and
    // would have failed to construct if it didn't hold) -- assert it
    // directly too, against the same pool, for a specific failure message.
    verify_db_schema(conn.pool()).await.unwrap();
}

#[tokio::test]
async fn test_db_indexes_created() {
    let dir = tempfile::tempdir().unwrap();
    let conn = DbConnection::new(dir.path().join("test.db")).await.unwrap();

    create_optimized_indexes(conn.pool()).await.unwrap();

    let index_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex_%'",
    )
    .fetch_one(conn.pool())
    .await
    .unwrap();
    assert_eq!(index_count, 5, "expected all 5 optimized indexes to exist");

    for index in [
        "idx_videos_status",
        "idx_videos_created_at",
        "idx_videos_status_created_at",
        "idx_videos_instance_id_status",
        "idx_conversion_logs_video_id",
    ] {
        let exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?",
        )
        .bind(index)
        .fetch_one(conn.pool())
        .await
        .unwrap();
        assert_eq!(exists, 1, "missing index {index}");
    }
}

#[tokio::test]
async fn test_db_migration_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    // First "boot": migrations run against a brand-new file.
    let conn1 = DbConnection::new(&db_path).await.unwrap();
    verify_db_schema(conn1.pool()).await.unwrap();
    let tables_after_first = count_tables(conn1.pool()).await.unwrap();
    let indexes_after_first = count_indexes(conn1.pool()).await.unwrap();
    drop(conn1);

    // Second "boot" (e.g. process restart): re-opening the same DB file
    // re-runs `sqlx::migrate!` and `create_optimized_indexes` against an
    // already-migrated database. Must not error, and must produce the exact
    // same schema.
    let conn2 = DbConnection::new(&db_path).await.unwrap();
    verify_db_schema(conn2.pool()).await.unwrap();
    create_optimized_indexes(conn2.pool()).await.unwrap();

    let tables_after_second = count_tables(conn2.pool()).await.unwrap();
    let indexes_after_second = count_indexes(conn2.pool()).await.unwrap();
    assert_eq!(tables_after_first, tables_after_second);
    assert_eq!(indexes_after_first, indexes_after_second);
}

// ---------------------------------------------------------------------
// 3. Preflight checks (#32)
// ---------------------------------------------------------------------

#[tokio::test]
async fn test_preflight_db_check_passes() {
    let dir = tempfile::tempdir().unwrap();
    let conn = DbConnection::new(dir.path().join("test.db")).await.unwrap();
    let config = common::build_config(dir.path());

    let checks = run_preflight_checks(&config, &conn).await;
    let db = checks.iter().find(|c| c.name == "Database").unwrap();
    assert_eq!(db.status, CheckStatus::Ok);
}

#[tokio::test]
async fn test_preflight_nas_unreachable_error() {
    let dir = tempfile::tempdir().unwrap();
    let conn = DbConnection::new(dir.path().join("test.db")).await.unwrap();
    let mut config = common::build_config(dir.path());
    // `NasConfig` has no separate port field -- SMB always dials the
    // standard port 445 via `smbclient` against `host`/`share`. Nothing
    // listens on 127.0.0.1:445 in this test environment, so this
    // deterministically fails the same way an unreachable
    // "127.0.0.1:9999"-style host would: connection refused / timeout.
    // A credential is set so the check reports a hard `Error` rather than
    // downgrading to `Warning` (see `nas_check`'s no-credential carve-out).
    config.nas.host = "127.0.0.1".to_string();
    config.nas.password = Some("pw".to_string());

    let checks = run_preflight_checks(&config, &conn).await;
    let nas = checks.iter().find(|c| c.name == "NAS").unwrap();
    assert_eq!(nas.status, CheckStatus::Error);
    assert!(
        !nas.message.is_empty(),
        "error message should explain the failure"
    );
}

#[tokio::test]
async fn test_preflight_discord_disabled_ok() {
    let dir = tempfile::tempdir().unwrap();
    let conn = DbConnection::new(dir.path().join("test.db")).await.unwrap();
    let mut config = common::build_config(dir.path());
    config.discord.enabled = false;

    let checks = run_preflight_checks(&config, &conn).await;
    let discord = checks.iter().find(|c| c.name == "Discord").unwrap();
    assert_eq!(discord.status, CheckStatus::Ok);
    assert_eq!(discord.message, "Disabled");
}

#[tokio::test]
async fn test_preflight_discord_invalid_webhook_error() {
    let dir = tempfile::tempdir().unwrap();
    let conn = DbConnection::new(dir.path().join("test.db")).await.unwrap();
    let mut config = common::build_config(dir.path());
    config.discord.enabled = true;
    config.discord.webhook_url = "not-a-valid-url".to_string();

    let checks = run_preflight_checks(&config, &conn).await;
    let discord = checks.iter().find(|c| c.name == "Discord").unwrap();
    assert_eq!(discord.status, CheckStatus::Error);
}

// ---------------------------------------------------------------------
// 4. Startup summary rendering (#33)
// ---------------------------------------------------------------------

#[test]
fn test_startup_summary_renders_without_panic() {
    let summary = common::sample_summary("render-test");
    let rendered = summary.render();

    assert!(rendered.contains("Trein Video - Master Startup Summary"));
    assert!(rendered.contains("Configuration"));
    assert!(rendered.contains("Database"));
    assert!(rendered.contains("NAS/SMB"));
    assert!(rendered.contains("Discord"));
    assert!(rendered.contains("API Server"));
    // Spot-check a couple of the section emoji the box is built from.
    assert!(rendered.contains('\u{1F4CA}')); // 📊 Configuration
    assert!(rendered.contains('\u{1F4C2}')); // 📂 NAS/SMB
    assert!(rendered.contains('\u{2705}')); // ✅ status marks
}

#[test]
fn test_startup_summary_contains_instance_id() {
    let summary = common::sample_summary("test-master");
    let rendered = summary.render();
    assert!(rendered.contains("test-master"));
}

#[test]
fn test_startup_summary_redacts_webhook() {
    // `redact_webhook` (see `src/startup/summary.rs`) never returns the
    // original URL or any part of the webhook token -- unlike
    // `NasConfig`'s password redaction (`***REDACTED***`), it collapses to
    // the literal string "configured". What matters here is the security
    // property, not the exact placeholder text: the real URL/token must
    // never appear in rendered output.
    let mut summary = common::sample_summary("redact-test");
    summary.discord = DiscordInfo {
        enabled: true,
        webhook_redacted: Some(redact_webhook(
            "https://discord.com/api/webhooks/999/super-secret-token",
        )),
        status: CheckStatus::Ok,
        message: "webhook OK".to_string(),
    };

    let rendered = summary.render();
    assert!(rendered.contains("Webhook valid"));
    assert!(!rendered.contains("super-secret-token"));
    assert!(!rendered.contains("https://discord.com"));
}

// ---------------------------------------------------------------------
// 5. Full master startup sequence
// ---------------------------------------------------------------------

/// Replays exactly the four steps `src/main.rs::run_master` performs before
/// starting mDNS/the API server -- config (#30), DB init + schema (#31),
/// preflight (#32), summary (#33) -- through the public API only.
///
/// There is no real SMB/NAS server in this test environment, so unlike
/// `run_master` in production this does **not** assert every check reports
/// `Ok`: the NAS check is expected to fail here (nothing listens on
/// 127.0.0.1:445), exactly as `test_preflight_nas_unreachable_error` already
/// establishes on its own. What this test actually proves is that the
/// *sequence itself* completes end-to-end without error or panic -- config
/// loads, the DB comes up with a verified schema and its indexes, all three
/// preflight checks run to completion and are reported, and the summary
/// renders -- which is the thing #29's sub-tickets (#30-#33) wire together
/// and #34 orchestrates.
#[tokio::test]
async fn test_master_startup_sequence_success() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("videos.db");
    let toml = common::master_toml(&db_path);
    let config_path = common::write_config(dir.path(), &toml);

    // --- #30: config ---
    let cfg = config::load_config(Some(config_path.clone())).unwrap();
    assert_eq!(cfg.instance.role, "master");

    // --- #31: DB schema + indexes ---
    let conn = DbConnection::new(&cfg.db.path).await.unwrap();
    verify_db_schema(conn.pool()).await.unwrap();
    create_optimized_indexes(conn.pool()).await.unwrap();
    let tables = count_tables(conn.pool()).await.unwrap();
    let indexes = count_indexes(conn.pool()).await.unwrap();
    assert_eq!(tables, 4);
    assert_eq!(indexes, 5);

    // --- #32: preflight ---
    let checks = run_preflight_checks(&cfg, &conn).await;
    assert_eq!(checks.len(), 3, "expected Database/NAS/Discord checks");
    let db_check = checks.iter().find(|c| c.name == "Database").unwrap();
    let nas_check = checks.iter().find(|c| c.name == "NAS").unwrap();
    let discord_check = checks.iter().find(|c| c.name == "Discord").unwrap();

    assert_eq!(db_check.status, CheckStatus::Ok);
    // Discord isn't configured in `common::master_toml` -> always Ok.
    assert_eq!(discord_check.status, CheckStatus::Ok);
    // NAS: see the doc comment above -- no real SMB server here, so this is
    // just confirmed present/reported, not asserted `Ok`.

    // --- #33: startup summary ---
    let summary = StartupSummary {
        instance_id: cfg.instance.id.clone(),
        config_path: config_path.clone(),
        codec: cfg.conversion.codec.clone(),
        preset: cfg.conversion.preset.clone(),
        max_parallel_jobs: cfg.conversion.max_parallel_jobs,
        db: DbInfo {
            tables,
            indexes,
            pool_size: 5,
            status: db_check.status,
            message: db_check.message.clone(),
        },
        nas: NasInfo {
            host: cfg.nas.host.clone(),
            share: cfg.nas.share.clone(),
            base_path: cfg.nas.base_path.clone(),
            file_count: None,
            status: nas_check.status,
            message: nas_check.message.clone(),
        },
        discord: DiscordInfo {
            enabled: cfg.discord.enabled,
            webhook_redacted: None,
            status: discord_check.status,
            message: discord_check.message.clone(),
        },
        api: ApiInfo {
            bind_addr: "0.0.0.0".to_string(),
            port: cfg.instance.api_port,
        },
    };

    let rendered = summary.render();
    assert!(rendered.contains("test-master"));
    assert!(rendered.contains(&config_path.display().to_string()));
    assert!(rendered.contains("4 tables, 5 indexes"));

    // Whatever the NAS outcome, DB is healthy and Discord is disabled, so a
    // critical failure can only ever come from the NAS check here -- assert
    // that's consistent rather than asserting a fixed pass/fail for it.
    assert_eq!(
        summary.has_critical_failure(),
        nas_check.status == CheckStatus::Error
    );
}

#[tokio::test]
async fn test_master_startup_worker_config_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("videos.db");
    let toml = common::worker_toml_missing_master_url(&db_path);
    let path = common::write_config(dir.path(), &toml);

    let err = config::load_config(Some(path)).unwrap_err();
    let message = format!("{err}").to_lowercase();
    assert!(
        message.contains("master_url"),
        "error should mention master_url, got: {message}"
    );
    assert!(
        message.contains("worker"),
        "error should mention worker, got: {message}"
    );
}

// ---------------------------------------------------------------------
// 6. Edge cases
// ---------------------------------------------------------------------

#[test]
fn test_config_default_path_resolution() {
    // Purely sync (path resolution does no I/O beyond reading env/home dir
    // lookups), so a plain `#[test]` is used rather than `#[tokio::test]`.
    // `None` is only possible when the platform has no resolvable home
    // directory (e.g. `$HOME` unset), which isn't the case in CI/dev.
    if let Some(path) = cli::default_config_path() {
        assert_eq!(path.file_name().unwrap(), "config.toml");
        assert!(path.is_absolute());
        if !cfg!(target_os = "windows") {
            let expected_suffix = Path::new(".config").join("trein-video").join("config.toml");
            assert!(
                path.ends_with(&expected_suffix),
                "expected path to end with {}, got {}",
                expected_suffix.display(),
                path.display()
            );
        }
    }
}

#[test]
fn test_config_tilde_expansion() {
    // Deliberately reads (never mutates) the real `$HOME` -- tests run in
    // parallel by default, and mutating process-global env vars would race
    // with `test_config_default_path_resolution` and friends.
    let Ok(home) = std::env::var("HOME") else {
        return; // nothing to assert without a real $HOME in this environment
    };

    let dir = tempfile::tempdir().unwrap();
    let toml = common::master_toml_with_tilde_db_path("trein-video-test-tilde.db");
    let path = common::write_config(dir.path(), &toml);

    let cfg = config::load_config(Some(path)).unwrap();
    assert_eq!(cfg.db.path, format!("{home}/trein-video-test-tilde.db"));
}

#[tokio::test]
async fn test_db_connection_pool_size() {
    let dir = tempfile::tempdir().unwrap();
    let conn = DbConnection::new(dir.path().join("test.db")).await.unwrap();

    // `DbConnection::new` configures `SqlitePoolOptions::max_connections(5)`
    // -- assert against the pool's actual configured options rather than a
    // hardcoded expectation duplicated from `src/db/connection.rs`.
    assert_eq!(conn.pool().options().get_max_connections(), 5);
}
