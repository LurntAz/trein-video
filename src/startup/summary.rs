//! Consolidated master startup summary (#33, sub-ticket of #29).
//!
//! Renders a single human-readable box combining the results of #30
//! (config path resolution), #31 (DB schema verification/indexes) and #32
//! (NAS/Discord/DB preflight checks) into one glanceable block, printed to
//! stdout in addition to (not instead of) the structured `tracing` log
//! lines each of those steps already emits -- the box is for a human
//! watching the terminal at boot, the logs are for everything else
//! (log aggregation, `LOG_FORMAT=json`, etc).

use std::path::{Path, PathBuf};

use is_terminal::IsTerminal;
use sqlx::SqlitePool;

use crate::startup::preflight::CheckStatus;

/// ANSI SGR codes. Only ever applied when stdout is a terminal (see
/// [`StartupSummary::render`]) -- the same TTY-detection precedent used by
/// `src/progress/display.rs` for its live-vs-plain rendering split.
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const RED: &str = "\x1b[31m";
const BOLD: &str = "\x1b[1m";
const RESET: &str = "\x1b[0m";

/// Interior width (in characters, between the two border columns) of the
/// rendered box. Matches the ticket's example art.
const WIDTH: usize = 56;

#[derive(Debug, Clone)]
pub struct DbInfo {
    pub tables: usize,
    pub indexes: usize,
    pub pool_size: u32,
    pub status: CheckStatus,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct NasInfo {
    pub host: String,
    pub share: String,
    pub base_path: String,
    /// `None` when the NAS was unreachable (no listing could be taken).
    pub file_count: Option<usize>,
    pub status: CheckStatus,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct DiscordInfo {
    pub enabled: bool,
    /// Never the real webhook URL -- see [`redact_webhook`].
    pub webhook_redacted: Option<String>,
    pub status: CheckStatus,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct ApiInfo {
    pub bind_addr: String,
    pub port: u16,
}

/// Everything the startup summary box needs, gathered once all of #30/#31/
/// #32's checks have run. Deliberately holds only display-ready data (no
/// live handles) so [`StartupSummary::render`] is a pure function.
#[derive(Debug, Clone)]
pub struct StartupSummary {
    pub instance_id: String,
    pub config_path: PathBuf,
    pub codec: String,
    pub preset: String,
    pub max_parallel_jobs: usize,
    pub db: DbInfo,
    pub nas: NasInfo,
    pub discord: DiscordInfo,
    pub api: ApiInfo,
}

impl StartupSummary {
    /// True if any check is severe enough that master startup must abort
    /// (DB or NAS in [`CheckStatus::Error`]). Discord is never critical --
    /// it only degrades notifications, never the pipeline itself.
    pub fn has_critical_failure(&self) -> bool {
        self.db.status == CheckStatus::Error || self.nas.status == CheckStatus::Error
    }

    /// Render the full boxed summary. Colorized with ANSI codes only when
    /// `stdout` is an actual terminal (`std::io::stdout().is_terminal()`),
    /// so output redirected to a log file stays plain, greppable text --
    /// same rule `src/progress/display.rs` already applies for progress
    /// bars vs. plain status lines.
    pub fn render(&self) -> String {
        self.render_with_color(std::io::stdout().is_terminal())
    }

    /// [`Self::render`] with the TTY-detection decision made explicit, for
    /// tests that need deterministic (uncolored) output.
    fn render_with_color(&self, color: bool) -> String {
        let mut out = String::new();

        out.push_str(&top_border());
        out.push('\n');
        out.push_str(&center_line("Trein Video - Master Startup Summary", color));
        out.push('\n');
        out.push_str(&mid_border());
        out.push('\n');
        out.push_str(&blank_line());
        out.push('\n');

        push_line(
            &mut out,
            &format!("  Instance ID:      {}", self.instance_id),
        );
        push_line(
            &mut out,
            &format!("  Config Path:      {}", self.config_path.display()),
        );
        out.push_str(&blank_line());
        out.push('\n');

        push_line(&mut out, "  \u{1F4CA} Configuration");
        push_line(
            &mut out,
            &format!("     - Codec:             {}", self.codec),
        );
        push_line(
            &mut out,
            &format!("     - Preset:            {}", self.preset),
        );
        push_line(
            &mut out,
            &format!("     - Max Parallel Jobs: {}", self.max_parallel_jobs),
        );
        out.push_str(&blank_line());
        out.push('\n');

        push_line(&mut out, "  \u{1F5C4}\u{FE0F}  Database");
        push_db_lines(&mut out, &self.db, color);
        out.push_str(&blank_line());
        out.push('\n');

        push_line(&mut out, "  \u{1F4C2} NAS/SMB");
        push_nas_lines(&mut out, &self.nas, color);
        out.push_str(&blank_line());
        out.push('\n');

        push_line(&mut out, "  \u{1F514} Discord");
        push_discord_lines(&mut out, &self.discord, color);
        out.push_str(&blank_line());
        out.push('\n');

        push_line(&mut out, "  \u{1F310} API Server");
        push_line(
            &mut out,
            &format!(
                "     {} Listening         ({}:{})",
                status_mark(CheckStatus::Ok, color),
                self.api.bind_addr,
                self.api.port
            ),
        );
        push_line(
            &mut out,
            &format!(
                "     {} Ready to accept jobs",
                status_mark(CheckStatus::Ok, color)
            ),
        );
        out.push_str(&blank_line());
        out.push('\n');

        out.push_str(&bottom_border());
        out
    }
}

fn push_db_lines(out: &mut String, db: &DbInfo, color: bool) {
    match db.status {
        CheckStatus::Ok => {
            push_line(
                out,
                &format!(
                    "     {} Schema verified   ({} tables, {} indexes)",
                    status_mark(CheckStatus::Ok, color),
                    db.tables,
                    db.indexes
                ),
            );
            push_line(
                out,
                &format!(
                    "     {} Connection OK     (pool: {} connections)",
                    status_mark(CheckStatus::Ok, color),
                    db.pool_size
                ),
            );
        }
        status => {
            push_line(
                out,
                &format!("     {} {}", status_mark(status, color), db.message),
            );
        }
    }
}

fn push_nas_lines(out: &mut String, nas: &NasInfo, color: bool) {
    // `host:base_path`, matching the ticket's own example
    // (`192.168.1.100:/videos`); the share name is already implicit in
    // `nas.message`/the preflight log lines (`//host/share/base_path`) for
    // anyone who needs the fully-qualified UNC form.
    let target = format!("{}:{}", nas.host, nas.base_path);
    match nas.status {
        CheckStatus::Error => {
            push_line(
                out,
                &format!(
                    "     {} Unreachable       ({target})",
                    status_mark(nas.status, color)
                ),
            );
            push_line(out, &format!("     {}", nas.message));
        }
        _ => {
            push_line(
                out,
                &format!(
                    "     {} Connection OK     ({target})",
                    status_mark(CheckStatus::Ok, color)
                ),
            );
            match nas.file_count {
                Some(count) => push_line(
                    out,
                    &format!(
                        "     {} Accessible        (listed {count} files)",
                        status_mark(nas.status, color)
                    ),
                ),
                None => push_line(
                    out,
                    &format!("     {} {}", status_mark(nas.status, color), nas.message),
                ),
            }
        }
    }
}

fn push_discord_lines(out: &mut String, discord: &DiscordInfo, color: bool) {
    if !discord.enabled {
        push_line(
            out,
            &format!(
                "     {} Disabled          (not configured)",
                status_mark(CheckStatus::Warning, color)
            ),
        );
        return;
    }

    match &discord.webhook_redacted {
        Some(redacted) if discord.status == CheckStatus::Ok => {
            push_line(
                out,
                &format!(
                    "     {} Webhook valid     ({redacted})",
                    status_mark(CheckStatus::Ok, color)
                ),
            );
        }
        _ => {
            push_line(
                out,
                &format!(
                    "     {} {}",
                    status_mark(discord.status, color),
                    discord.message
                ),
            );
        }
    }
}

fn status_mark(status: CheckStatus, color: bool) -> String {
    let (icon, code) = match status {
        CheckStatus::Ok => ("\u{2705}", GREEN),
        CheckStatus::Warning => ("\u{26A0}\u{FE0F}", YELLOW),
        CheckStatus::Error => ("\u{274C}", RED),
    };
    if color {
        format!("{code}{icon}{RESET}")
    } else {
        icon.to_string()
    }
}

fn top_border() -> String {
    format!("\u{2554}{}\u{2557}", "\u{2550}".repeat(WIDTH))
}

fn mid_border() -> String {
    format!("\u{2560}{}\u{2563}", "\u{2550}".repeat(WIDTH))
}

fn bottom_border() -> String {
    format!("\u{255A}{}\u{255D}", "\u{2550}".repeat(WIDTH))
}

fn blank_line() -> String {
    format!("\u{2551}{}\u{2551}", " ".repeat(WIDTH))
}

/// Center `text` within [`WIDTH`] and wrap it in `║ ... ║`, bolded.
fn center_line(text: &str, color: bool) -> String {
    let len = text.chars().count();
    let total_pad = WIDTH.saturating_sub(len);
    let left = total_pad / 2;
    let right = total_pad - left;
    let body = if color {
        format!("{BOLD}{text}{RESET}")
    } else {
        text.to_string()
    };
    format!(
        "\u{2551}{}{body}{}\u{2551}",
        " ".repeat(left),
        " ".repeat(right)
    )
}

/// Append one content line, left-padded to [`WIDTH`] and wrapped in
/// `║ ... ║`. `text` may contain multi-byte (emoji) characters; padding is
/// computed on `chars().count()`, which is an approximation of on-screen
/// width (wide emoji glyphs can still throw off terminal alignment by a
/// column or two) but keeps this dependency-free.
///
/// Some check messages (e.g. `smbclient`'s multi-line stderr surfaced
/// through `PreflightError::Nas`) can contain embedded newlines, which
/// would otherwise break out of the box entirely -- those are collapsed to
/// single spaces first so every logical "line" here really is one row of
/// the box, even if the underlying message spanned several.
fn push_line(out: &mut String, text: &str) {
    // Collapse embedded newlines down to a single space -- but only when
    // `text` actually contains one, so normal single-line calls keep their
    // intentional leading/column-alignment spaces (e.g.
    // "  Codec:             av1") untouched.
    let text: String = if text.contains(['\n', '\r']) {
        text.split(['\n', '\r'])
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join(" ")
    } else {
        text.to_string()
    };
    let len = text.chars().count();
    let pad = WIDTH.saturating_sub(len);
    out.push('\u{2551}');
    out.push_str(&text);
    out.push_str(&" ".repeat(pad));
    out.push_str("\u{2551}\n");
}

/// Redact a Discord webhook URL for display: never show the token, only
/// that one is configured. Mirrors the redaction precedent already applied
/// to `NasConfig::password` (`src/config.rs`) and the "never log the
/// webhook URL" comment in `main.rs::run_worker`.
pub fn redact_webhook(webhook_url: &str) -> String {
    let _ = webhook_url;
    "configured".to_string()
}

/// Count user tables (excluding SQLite's own internal `sqlite_%` tables) in
/// `pool`. Used for the "N tables" figure in the summary; the exact set of
/// expected tables/indexes is still authoritatively checked by
/// `db::connection::verify_db_schema`/`create_optimized_indexes` -- this is
/// display-only.
pub async fn count_tables(pool: &SqlitePool) -> Result<usize, sqlx::Error> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
    )
    .fetch_one(pool)
    .await?;
    Ok(count as usize)
}

/// Count indexes (excluding SQLite's own auto-generated `sqlite_autoindex_%`
/// ones backing `PRIMARY KEY`/`UNIQUE` constraints) in `pool`.
pub async fn count_indexes(pool: &SqlitePool) -> Result<usize, sqlx::Error> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex_%'",
    )
    .fetch_one(pool)
    .await?;
    Ok(count as usize)
}

/// The default config path shown in the summary when no explicit `--config`
/// was passed (mirrors `cli::default_config_path`'s own doc comment for the
/// per-platform layout), formatted for display purposes only.
pub fn display_config_path(path: &Path) -> String {
    path.display().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok_db() -> DbInfo {
        DbInfo {
            tables: 4,
            indexes: 5,
            pool_size: 5,
            status: CheckStatus::Ok,
            message: "connection OK".to_string(),
        }
    }

    fn ok_nas() -> NasInfo {
        NasInfo {
            host: "192.168.1.100".to_string(),
            share: "videos".to_string(),
            base_path: "/movies".to_string(),
            file_count: Some(42),
            status: CheckStatus::Ok,
            message: "reachable".to_string(),
        }
    }

    fn disabled_discord() -> DiscordInfo {
        DiscordInfo {
            enabled: false,
            webhook_redacted: None,
            status: CheckStatus::Ok,
            message: "Disabled".to_string(),
        }
    }

    fn base_summary() -> StartupSummary {
        StartupSummary {
            instance_id: "master-1".to_string(),
            config_path: PathBuf::from("/home/user/.config/trein-video/config.toml"),
            codec: "av1".to_string(),
            preset: "slow".to_string(),
            max_parallel_jobs: 3,
            db: ok_db(),
            nas: ok_nas(),
            discord: disabled_discord(),
            api: ApiInfo {
                bind_addr: "0.0.0.0".to_string(),
                port: 8000,
            },
        }
    }

    #[test]
    fn test_render_contains_all_sections() {
        let summary = base_summary();
        let rendered = summary.render_with_color(false);

        assert!(rendered.contains("Trein Video - Master Startup Summary"));
        assert!(rendered.contains("master-1"));
        assert!(rendered.contains("config.toml"));
        assert!(rendered.contains("Configuration"));
        assert!(rendered.contains("av1"));
        assert!(rendered.contains("slow"));
        assert!(rendered.contains('3'));
        assert!(rendered.contains("Database"));
        assert!(rendered.contains("4 tables, 5 indexes"));
        assert!(rendered.contains("pool: 5 connections"));
        assert!(rendered.contains("NAS/SMB"));
        assert!(rendered.contains("listed 42 files"));
        assert!(rendered.contains("Discord"));
        assert!(rendered.contains("Disabled"));
        assert!(rendered.contains("API Server"));
        assert!(rendered.contains("0.0.0.0:8000"));
        assert!(rendered.contains("Ready to accept jobs"));
    }

    #[test]
    fn test_render_is_a_closed_box() {
        let rendered = base_summary().render_with_color(false);
        let lines: Vec<&str> = rendered.lines().collect();
        assert!(lines.first().unwrap().starts_with('\u{2554}'));
        assert!(lines.last().unwrap().starts_with('\u{255A}'));
        for line in &lines[1..lines.len() - 1] {
            assert!(
                line.starts_with('\u{2551}') || line.starts_with('\u{2560}'),
                "line not part of the box: {line}"
            );
        }
    }

    #[test]
    fn test_render_without_color_has_no_ansi_escapes() {
        let rendered = base_summary().render_with_color(false);
        assert!(!rendered.contains('\x1b'));
    }

    #[test]
    fn test_render_with_color_has_ansi_escapes() {
        let rendered = base_summary().render_with_color(true);
        assert!(rendered.contains('\x1b'));
    }

    #[test]
    fn test_render_shows_error_status_for_failed_db() {
        let mut summary = base_summary();
        summary.db = DbInfo {
            tables: 0,
            indexes: 0,
            pool_size: 5,
            status: CheckStatus::Error,
            message: "database query failed: unable to open database file".to_string(),
        };
        let rendered = summary.render_with_color(false);
        assert!(rendered.contains("unable to open database file"));
    }

    #[test]
    fn test_render_shows_enabled_discord_webhook() {
        let mut summary = base_summary();
        summary.discord = DiscordInfo {
            enabled: true,
            webhook_redacted: Some(redact_webhook(
                "https://discord.com/api/webhooks/123/super-secret-token",
            )),
            status: CheckStatus::Ok,
            message: "webhook OK".to_string(),
        };
        let rendered = summary.render_with_color(false);
        assert!(rendered.contains("Webhook valid"));
        assert!(!rendered.contains("super-secret-token"));
    }

    #[test]
    fn test_has_critical_failure_true_on_db_error() {
        let mut summary = base_summary();
        summary.db.status = CheckStatus::Error;
        assert!(summary.has_critical_failure());
    }

    #[test]
    fn test_has_critical_failure_true_on_nas_error() {
        let mut summary = base_summary();
        summary.nas.status = CheckStatus::Error;
        assert!(summary.has_critical_failure());
    }

    #[test]
    fn test_has_critical_failure_false_when_only_discord_warns() {
        let mut summary = base_summary();
        summary.discord.status = CheckStatus::Warning;
        assert!(!summary.has_critical_failure());
    }

    #[test]
    fn test_redact_webhook_never_contains_original_url() {
        let redacted = redact_webhook("https://discord.com/api/webhooks/123/abcdef");
        assert!(!redacted.contains("abcdef"));
    }

    #[tokio::test]
    async fn test_count_tables_and_indexes_against_real_db() {
        use crate::db::DbConnection;

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = DbConnection::new(&db_path).await.unwrap();

        let tables = count_tables(conn.pool()).await.unwrap();
        let indexes = count_indexes(conn.pool()).await.unwrap();

        // videos, instances, conversion_logs, _sqlx_migrations -- see
        // EXPECTED_TABLES in db::connection. `_sqlx_migrations` isn't
        // `sqlite_%`-prefixed (that prefix is reserved for SQLite's own
        // internal objects), so it counts too.
        assert_eq!(tables, 4, "expected the 4 EXPECTED_TABLES, got {tables}");
        assert_eq!(
            indexes, 5,
            "expected the 5 indexes created by create_optimized_indexes"
        );
    }

    #[test]
    fn test_display_config_path_shows_full_path() {
        let path = PathBuf::from("/home/user/.config/trein-video/config.toml");
        assert_eq!(
            display_config_path(&path),
            "/home/user/.config/trein-video/config.toml"
        );
    }
}
