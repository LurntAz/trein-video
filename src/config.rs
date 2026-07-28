use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub instance: InstanceConfig,
    pub nas: NasConfig,
    pub conversion: ConversionConfig,
    pub sync: SyncConfig,
    pub db: DbConfig,
    pub tls: TlsConfig,
    pub discovery: DiscoveryConfig,
    #[serde(default)]
    pub video_discovery: VideoDiscoveryConfig,
    #[serde(default)]
    pub retry: RetryConfig,
    #[serde(default)]
    pub discord: DiscordConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceConfig {
    pub id: String,
    pub role: String, // "master" or "worker"
    pub api_port: u16,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct NasConfig {
    pub protocol: String, // "smb"
    pub host: String,
    pub share: String,
    pub username: String,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub password_env: Option<String>,
    pub base_path: String,
}

impl NasConfig {
    /// Get the actual password: either from `password` field, or from the env var in `password_env`.
    pub fn get_password(&self) -> Option<String> {
        if let Some(pwd) = &self.password {
            return Some(pwd.clone());
        }
        if let Some(env_var) = &self.password_env {
            return std::env::var(env_var).ok();
        }
        None
    }
}

/// Custom `Debug` impl that never prints the NAS password, even if this
/// struct (or a struct that embeds it) ends up in a `{:?}`/`tracing` log.
impl std::fmt::Debug for NasConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NasConfig")
            .field("protocol", &self.protocol)
            .field("host", &self.host)
            .field("share", &self.share)
            .field("username", &self.username)
            .field(
                "password",
                &self.password.as_ref().map(|_| "***REDACTED***"),
            )
            .field("base_path", &self.base_path)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversionConfig {
    pub codec: String, // "av1" or "h265"
    pub preset: String,
    pub crf: u8,
    pub max_parallel_jobs: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    pub poll_interval_secs: u64,
    pub master_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbConfig {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
    pub ca_cert_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    pub enabled: bool,
    pub service_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoDiscoveryConfig {
    #[serde(default = "default_video_discovery_enabled")]
    pub enabled: bool,
    #[serde(default = "default_video_discovery_interval")]
    pub interval_secs: u64,
}

fn default_video_discovery_enabled() -> bool {
    true
}

fn default_video_discovery_interval() -> u64 {
    3600
}

/// Discord webhook notifications for completed video conversions. Used by
/// the worker's pipeline to post a message when a job finishes.
///
/// `Config::discord` is `#[serde(default)]`, so an existing `config.toml`
/// without a `[discord]` section still parses (and comes up with the
/// feature disabled) rather than failing to load. That's why
/// `DiscordConfig::default()` sets `enabled: false` even though the
/// per-field default for `enabled` *within* an explicit `[discord]` section
/// is `true` (i.e. adding `[discord]\nwebhook_url = "..."` without an
/// `enabled` key turns the feature on).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiscordConfig {
    #[serde(default = "default_discord_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub webhook_url: String,
}

fn default_discord_enabled() -> bool {
    true
}

/// Retry policy (#17) shared by the worker's pipeline (download/upload
/// stages, `worker::processor`) and, indirectly, its error-classification
/// helpers in `error::retry`. `#[serde(default)]` on `Config::retry` means
/// an omitted `[retry]` section in an existing `config.toml` falls back to
/// these defaults rather than failing to parse.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetryConfig {
    /// Total attempts (including the first) before giving up. Ticket #17
    /// specifies 5.
    pub max_attempts: u32,
    pub base_delay_secs: u64,
    /// Backoff never exceeds this, no matter how many attempts have
    /// elapsed. Ticket #17 specifies a 5 minute cap.
    pub max_delay_secs: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay_secs: 1,
            max_delay_secs: 300,
        }
    }
}

impl Default for VideoDiscoveryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 3600,
        }
    }
}

fn expand_tilde_path(path: &str) -> String {
    if path.starts_with("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return path.replacen("~", &home, 1);
        }
    }
    path.to_string()
}

pub fn load_config<P: AsRef<Path>>(path: P) -> Result<Config> {
    let content = std::fs::read_to_string(path)?;
    let mut config: Config = toml::from_str(&content)?;

    // Expand ~ in all file paths
    config.db.path = expand_tilde_path(&config.db.path);
    config.tls.cert_path = expand_tilde_path(&config.tls.cert_path);
    config.tls.key_path = expand_tilde_path(&config.tls.key_path);
    config.tls.ca_cert_path = expand_tilde_path(&config.tls.ca_cert_path);
    // `~` has no meaning in an HTTP(S) webhook URL, but we expand it here for
    // consistency with the other path-like fields above (e.g. a webhook URL
    // pasted from a file under `~/...` by mistake still gets normalized the
    // same way, and the behavior stays uniform across all string fields this
    // function touches).
    config.discord.webhook_url = expand_tilde_path(&config.discord.webhook_url);

    validate_config(&config)?;
    Ok(config)
}

fn validate_config(config: &Config) -> Result<()> {
    if config.instance.role != "master" && config.instance.role != "worker" {
        anyhow::bail!(
            "Invalid role: {}. Must be 'master' or 'worker'",
            config.instance.role
        );
    }

    if config.nas.protocol != "smb" {
        anyhow::bail!("Only SMB/CIFS protocol is supported currently");
    }

    if config.conversion.codec != "av1" && config.conversion.codec != "h265" {
        anyhow::bail!("Codec must be 'av1' or 'h265'");
    }

    if config.instance.role == "worker" && config.sync.master_url.is_none() {
        anyhow::bail!("Worker instances must specify sync.master_url");
    }

    if config.retry.max_attempts == 0 {
        anyhow::bail!("retry.max_attempts must be at least 1");
    }

    if config.discord.enabled {
        match reqwest::Url::parse(&config.discord.webhook_url) {
            Ok(url) if url.scheme() == "http" || url.scheme() == "https" => {}
            Ok(url) => {
                anyhow::bail!(
                    "discord.webhook_url must be an http(s) URL, got scheme '{}'",
                    url.scheme()
                );
            }
            Err(_) => {
                anyhow::bail!(
                    "discord.webhook_url is not a valid URL: '{}'",
                    config.discord.webhook_url
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_config() {
        // Config will be tested with actual TOML file
    }

    #[test]
    fn test_nas_config_debug_redacts_password() {
        let nas = NasConfig {
            protocol: "smb".to_string(),
            host: "192.168.1.100".to_string(),
            share: "videos".to_string(),
            username: "user".to_string(),
            password: Some("super-secret".to_string()),
            base_path: "/videos".to_string(),
        };
        let debug_output = format!("{:?}", nas);
        assert!(!debug_output.contains("super-secret"));
        assert!(debug_output.contains("REDACTED"));
    }

    #[test]
    fn test_retry_config_default_matches_ticket_17_spec() {
        let retry = RetryConfig::default();
        assert_eq!(retry.max_attempts, 5);
        assert_eq!(retry.max_delay_secs, 300);
    }

    #[test]
    fn test_discord_config_default_is_disabled() {
        // No [discord] section in the TOML => Config::discord falls back to
        // DiscordConfig::default(), which must be disabled so that existing
        // config files (predating this feature) keep validating.
        let discord = DiscordConfig::default();
        assert!(!discord.enabled);
        assert_eq!(discord.webhook_url, "");
    }

    #[test]
    fn test_discord_enabled_key_defaults_to_true_within_explicit_section() {
        // Per-field default: an explicit [discord] section that only sets
        // webhook_url should come up enabled.
        let discord: DiscordConfig =
            toml::from_str(r#"webhook_url = "https://discord.com/api/webhooks/1/abc""#).unwrap();
        assert!(discord.enabled);
        assert_eq!(
            discord.webhook_url,
            "https://discord.com/api/webhooks/1/abc"
        );
    }

    fn base_config_for_validation() -> Config {
        Config {
            instance: InstanceConfig {
                id: "test".to_string(),
                role: "master".to_string(),
                api_port: 8000,
            },
            nas: NasConfig {
                protocol: "smb".to_string(),
                host: "host".to_string(),
                share: "share".to_string(),
                username: "user".to_string(),
                password: Some("pw".to_string()),
                password_env: None,
                base_path: "/videos".to_string(),
            },
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
                path: "/tmp/videos.db".to_string(),
            },
            tls: TlsConfig {
                cert_path: "/tmp/cert".to_string(),
                key_path: "/tmp/key".to_string(),
                ca_cert_path: "/tmp/ca".to_string(),
            },
            discovery: DiscoveryConfig {
                enabled: true,
                service_name: "svc".to_string(),
            },
            video_discovery: VideoDiscoveryConfig::default(),
            retry: RetryConfig::default(),
            discord: DiscordConfig::default(),
        }
    }

    #[test]
    fn test_validate_rejects_invalid_webhook_url_when_enabled() {
        let mut config = base_config_for_validation();
        config.discord.enabled = true;
        config.discord.webhook_url = "not a url".to_string();
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_rejects_non_http_webhook_scheme() {
        let mut config = base_config_for_validation();
        config.discord.enabled = true;
        config.discord.webhook_url = "ftp://discord.com/webhooks/1".to_string();
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_accepts_valid_https_webhook_url() {
        let mut config = base_config_for_validation();
        config.discord.enabled = true;
        config.discord.webhook_url = "https://discord.com/api/webhooks/1/abc".to_string();
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_ignores_webhook_url_when_disabled() {
        let mut config = base_config_for_validation();
        config.discord.enabled = false;
        config.discord.webhook_url = "".to_string();
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_nas_config_debug_no_password() {
        let nas = NasConfig {
            protocol: "smb".to_string(),
            host: "192.168.1.100".to_string(),
            share: "videos".to_string(),
            username: "user".to_string(),
            password: None,
            base_path: "/videos".to_string(),
        };
        let debug_output = format!("{:?}", nas);
        assert!(debug_output.contains("password: None"));
    }
}
