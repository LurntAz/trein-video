//! Connection preflight checks (#32, sub-ticket of #29).
//!
//! Validates, at master startup, that the critical external dependencies
//! (database, NAS/SMB share) are reachable, and that the optional Discord
//! webhook (if configured) looks valid. Every check is independent and
//! bounded by a short timeout so an unreachable NAS or Discord outage can
//! never hang startup indefinitely.
//!
//! Mirrors the existing `check_required_binaries()` precedent in
//! `src/main.rs`: best-effort, non-fatal, logged and surfaced to the
//! operator, but never a reason to abort master startup on its own — a
//! temporarily-down NAS shouldn't prevent the API server (and mDNS
//! discovery, sync coordination with workers) from coming up. Whether/how a
//! human-facing "startup summary" upgrades an `Error` here into an actual
//! abort is left to the master startup orchestration ticket (#34); this
//! module only produces the results.
//!
//! The Discord check deliberately only ever does a `GET` on the webhook URL
//! to confirm it resolves to a real webhook object -- it must never `POST` a
//! message, since that's the worker's job on job completion (see
//! `src/discord/mod.rs`).

use std::time::Duration;

use thiserror::Error;
use tracing::{error, info, warn};

use crate::config::{Config, DiscordConfig, NasConfig};
use crate::db::DbConnection;
use crate::nas::SmbClient;

/// Bound on how long the NAS probe may take before it's treated as a
/// failure. `smbclient` itself has no reliable non-interactive timeout flag,
/// so this is enforced from our side via `tokio::time::timeout`.
const NAS_CHECK_TIMEOUT: Duration = Duration::from_secs(5);

/// Bound on how long the Discord webhook `GET` may take.
const DISCORD_CHECK_TIMEOUT: Duration = Duration::from_secs(5);

/// Errors from an individual preflight check. Always carries a
/// human-readable, actionable message -- never the raw NAS password or the
/// full Discord webhook URL (both are bearer credentials, same sensitivity
/// class enforced elsewhere by `NasConfig`'s custom `Debug` impl and the
/// webhook-URL-logging comment in `main.rs::run_worker`).
#[derive(Debug, Error)]
pub enum PreflightError {
    #[error("database query failed: {0}")]
    Database(String),
    #[error("NAS //{host}/{share} unreachable: {detail}")]
    Nas {
        host: String,
        share: String,
        detail: String,
    },
    #[error("NAS check timed out after {0:?}")]
    NasTimeout(Duration),
    #[error("discord.webhook_url is not a valid URL: {0}")]
    DiscordInvalidUrl(String),
    #[error("discord webhook check failed: {0}")]
    Discord(String),
    #[error("discord webhook check timed out after {0:?}")]
    DiscordTimeout(Duration),
}

/// Result of a single check, with no presentation baked in -- suitable for
/// composing into the consolidated startup summary (#34) or driving a hard
/// abort, at the caller's discretion.
pub type CheckResult = Result<(), PreflightError>;

/// Severity for CLI/log display. `Warning` is reserved for conditions that
/// are worth an operator's attention but are not necessarily wrong (e.g. no
/// NAS password configured -- some shares are genuinely anonymous).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckStatus {
    Ok,
    Warning,
    Error,
}

#[derive(Debug, Clone)]
pub struct PreflightCheck {
    pub name: &'static str,
    pub status: CheckStatus,
    pub message: String,
}

/// Cheap database liveness check: a bare `SELECT 1` against the pool. Does
/// not attempt full schema verification (see #31 for the migration-time
/// `verify_schema` step) -- this only confirms the connection the master is
/// about to rely on for the rest of its lifetime actually works.
pub async fn check_db(conn: &DbConnection) -> CheckResult {
    sqlx::query("SELECT 1")
        .execute(conn.pool())
        .await
        .map(|_| ())
        .map_err(|e| PreflightError::Database(e.to_string()))
}

/// Lightweight SMB connectivity probe: a non-recursive listing of
/// `nas.base_path`'s immediate contents, reusing `SmbClient` exactly as the
/// worker/video-discovery paths do. Bounded by [`NAS_CHECK_TIMEOUT`] so an
/// unreachable host can't hang startup.
pub async fn check_nas(nas: &NasConfig) -> CheckResult {
    let client = SmbClient::new(
        nas.host.clone(),
        nas.share.clone(),
        nas.username.clone(),
        nas.get_password(),
    );

    match tokio::time::timeout(NAS_CHECK_TIMEOUT, client.list_videos(&nas.base_path)).await {
        Ok(Ok(_entries)) => Ok(()),
        Ok(Err(e)) => Err(PreflightError::Nas {
            host: nas.host.clone(),
            share: nas.share.clone(),
            detail: e.to_string(),
        }),
        Err(_elapsed) => Err(PreflightError::NasTimeout(NAS_CHECK_TIMEOUT)),
    }
}

/// Discord webhook validity check. Skips (returns `Ok`) when Discord isn't
/// enabled -- mirrors `DiscordConfig::default()` semantics in
/// `src/config.rs`, where "no `[discord]` section" means "feature off", not
/// "feature broken". When enabled, `GET`s the webhook URL (never `POST`) and
/// treats any 2xx response as a valid webhook.
pub async fn check_discord(discord: &DiscordConfig) -> CheckResult {
    if !discord.enabled {
        return Ok(());
    }

    let url = reqwest::Url::parse(&discord.webhook_url)
        .map_err(|e| PreflightError::DiscordInvalidUrl(e.to_string()))?;

    let client = reqwest::Client::builder()
        .timeout(DISCORD_CHECK_TIMEOUT)
        .build()
        .map_err(|e| PreflightError::Discord(e.to_string()))?;

    match client.get(url).send().await {
        Ok(resp) if resp.status().is_success() => Ok(()),
        Ok(resp) => Err(PreflightError::Discord(format!(
            "webhook returned HTTP {}",
            resp.status()
        ))),
        Err(e) if e.is_timeout() => Err(PreflightError::DiscordTimeout(DISCORD_CHECK_TIMEOUT)),
        Err(e) => Err(PreflightError::Discord(e.to_string())),
    }
}

/// Run all preflight checks and return a display-ready summary. Never
/// blocks longer than `NAS_CHECK_TIMEOUT + DISCORD_CHECK_TIMEOUT` combined
/// (the DB check has no timeout of its own -- it's a local/fast query
/// against a pool that already enforces `busy_timeout`, see
/// `db::connection::DbConnection::new`).
///
/// Each check is also logged immediately via `tracing`, at a level matching
/// its status, so the outcome shows up in master's startup logs even before
/// any caller inspects the returned `Vec`.
pub async fn run_preflight_checks(config: &Config, conn: &DbConnection) -> Vec<PreflightCheck> {
    let checks = vec![
        db_check(conn).await,
        nas_check(&config.nas).await,
        discord_check(&config.discord).await,
    ];

    for check in &checks {
        log_check(check);
    }

    checks
}

async fn db_check(conn: &DbConnection) -> PreflightCheck {
    match check_db(conn).await {
        Ok(()) => PreflightCheck {
            name: "Database",
            status: CheckStatus::Ok,
            message: "connection OK".to_string(),
        },
        Err(e) => PreflightCheck {
            name: "Database",
            status: CheckStatus::Error,
            message: e.to_string(),
        },
    }
}

async fn nas_check(nas: &NasConfig) -> PreflightCheck {
    let nas_path = format!("//{}/{}{}", nas.host, nas.share, nas.base_path);
    let has_credential = nas.password.is_some() || nas.password_env.is_some();

    match check_nas(nas).await {
        Ok(()) => PreflightCheck {
            name: "NAS",
            status: CheckStatus::Ok,
            message: format!("{nas_path} reachable"),
        },
        // No password configured at all (neither `password` nor
        // `password_env`) is an incomplete-config situation, not
        // necessarily a broken one -- some shares are genuinely anonymous.
        // Downgrade to a warning so it doesn't read as a hard failure.
        Err(e) if !has_credential => PreflightCheck {
            name: "NAS",
            status: CheckStatus::Warning,
            message: format!(
                "{nas_path}: no password configured (password/password_env unset) and \
                 connection failed: {e}"
            ),
        },
        Err(e) => PreflightCheck {
            name: "NAS",
            status: CheckStatus::Error,
            message: format!("{nas_path}: {e}"),
        },
    }
}

async fn discord_check(discord: &DiscordConfig) -> PreflightCheck {
    if !discord.enabled {
        return PreflightCheck {
            name: "Discord",
            status: CheckStatus::Ok,
            message: "Disabled".to_string(),
        };
    }

    match check_discord(discord).await {
        Ok(()) => PreflightCheck {
            name: "Discord",
            status: CheckStatus::Ok,
            message: "webhook OK".to_string(),
        },
        // A malformed URL is a config mistake -- actionable and worth a
        // hard error. A network hiccup or a non-2xx response could well be
        // transient (Discord flaky, webhook temporarily rate-limited, ...),
        // so those are surfaced as warnings instead.
        Err(e @ PreflightError::DiscordInvalidUrl(_)) => PreflightCheck {
            name: "Discord",
            status: CheckStatus::Error,
            message: e.to_string(),
        },
        Err(e) => PreflightCheck {
            name: "Discord",
            status: CheckStatus::Warning,
            message: e.to_string(),
        },
    }
}

fn log_check(check: &PreflightCheck) {
    match check.status {
        CheckStatus::Ok => info!(check = check.name, "\u{2705} {} ... OK", check.name),
        CheckStatus::Warning => warn!(
            check = check.name,
            "\u{26a0}\u{fe0f}  {} ... WARNING: {}", check.name, check.message
        ),
        CheckStatus::Error => error!(
            check = check.name,
            "\u{274c} {} ... ERROR: {}", check.name, check.message
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        ConversionConfig, DbConfig, DiscoveryConfig, InstanceConfig, RetryConfig, SyncConfig,
        TlsConfig, VideoDiscoveryConfig,
    };

    fn test_nas_config() -> NasConfig {
        NasConfig {
            protocol: "smb".to_string(),
            // Nothing listens on localhost:445 in the test environment, so
            // this fails fast with a connection error instead of hanging
            // until the DNS/TCP timeout for a bogus hostname.
            host: "127.0.0.1".to_string(),
            share: "share".to_string(),
            username: "user".to_string(),
            password: Some("pw".to_string()),
            password_env: None,
            base_path: "/videos".to_string(),
        }
    }

    fn base_config() -> Config {
        Config {
            instance: InstanceConfig {
                id: "test".to_string(),
                role: "master".to_string(),
                api_port: 8000,
            },
            nas: test_nas_config(),
            conversion: ConversionConfig {
                codec: "av1".to_string(),
                preset: "slow".to_string(),
                crf: 32,
                max_parallel_jobs: 1,
            },
            sync: SyncConfig {
                poll_interval_secs: 30,
                master_url: None,
            },
            db: DbConfig {
                path: "/tmp/preflight-test.db".to_string(),
            },
            tls: TlsConfig {
                cert_path: "/tmp/cert".to_string(),
                key_path: "/tmp/key".to_string(),
                ca_cert_path: "/tmp/ca".to_string(),
            },
            discovery: DiscoveryConfig {
                enabled: false,
                service_name: "svc".to_string(),
            },
            video_discovery: VideoDiscoveryConfig::default(),
            retry: RetryConfig::default(),
            discord: DiscordConfig::default(),
        }
    }

    #[tokio::test]
    async fn test_check_db_ok_on_healthy_connection() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = DbConnection::new(&db_path).await.unwrap();

        assert!(check_db(&conn).await.is_ok());
    }

    #[tokio::test]
    async fn test_run_preflight_checks_reports_db_ok() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = DbConnection::new(&db_path).await.unwrap();
        let config = base_config();

        let results = run_preflight_checks(&config, &conn).await;
        let db_result = results.iter().find(|c| c.name == "Database").unwrap();
        assert_eq!(db_result.status, CheckStatus::Ok);
    }

    #[tokio::test]
    async fn test_check_nas_unreachable_host_returns_error() {
        let nas = test_nas_config();
        let result = check_nas(&nas).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nas_check_downgrades_to_warning_without_credentials() {
        let mut nas = test_nas_config();
        nas.password = None;
        nas.password_env = None;

        let check = nas_check(&nas).await;
        assert_eq!(check.status, CheckStatus::Warning);
        assert!(check.message.contains("no password configured"));
    }

    #[tokio::test]
    async fn test_nas_check_reports_error_with_credentials_configured() {
        // Password is set but the host is unreachable, so this should be a
        // hard error, not a warning.
        let nas = test_nas_config();
        let check = nas_check(&nas).await;
        assert_eq!(check.status, CheckStatus::Error);
        assert!(check.message.contains(&nas.base_path));
    }

    #[tokio::test]
    async fn test_check_discord_disabled_is_ok() {
        let discord = DiscordConfig {
            enabled: false,
            webhook_url: String::new(),
        };
        assert!(check_discord(&discord).await.is_ok());

        let check = discord_check(&discord).await;
        assert_eq!(check.status, CheckStatus::Ok);
        assert_eq!(check.message, "Disabled");
    }

    #[tokio::test]
    async fn test_check_discord_invalid_url_is_error() {
        let discord = DiscordConfig {
            enabled: true,
            webhook_url: "not a url".to_string(),
        };

        let result = check_discord(&discord).await;
        assert!(matches!(result, Err(PreflightError::DiscordInvalidUrl(_))));

        let check = discord_check(&discord).await;
        assert_eq!(check.status, CheckStatus::Error);
    }

    #[tokio::test]
    async fn test_check_discord_valid_webhook_is_ok() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/webhook")
            .with_status(200)
            .with_body(r#"{"id":"123","type":1,"channel_id":"456"}"#)
            .create_async()
            .await;

        let discord = DiscordConfig {
            enabled: true,
            webhook_url: format!("{}/webhook", server.url()),
        };

        assert!(check_discord(&discord).await.is_ok());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_check_discord_non_success_status_is_warning() {
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("GET", "/webhook")
            .with_status(404)
            .create_async()
            .await;

        let discord = DiscordConfig {
            enabled: true,
            webhook_url: format!("{}/webhook", server.url()),
        };

        let result = check_discord(&discord).await;
        assert!(result.is_err());

        let check = discord_check(&discord).await;
        assert_eq!(check.status, CheckStatus::Warning);
    }

    #[tokio::test]
    async fn test_check_discord_never_sends_post() {
        // Only register a GET mock. If `check_discord` ever posted instead
        // (e.g. a future refactor accidentally reusing the worker's
        // notification path), mockito would 501 on the unmatched POST and
        // this test would fail via the non-success branch.
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/webhook")
            .with_status(200)
            .with_body("{}")
            .expect(1)
            .create_async()
            .await;

        let discord = DiscordConfig {
            enabled: true,
            webhook_url: format!("{}/webhook", server.url()),
        };

        let _ = check_discord(&discord).await;
        mock.assert_async().await;
    }
}
