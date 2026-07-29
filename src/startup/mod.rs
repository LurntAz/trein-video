//! Master startup helpers (#29).
//!
//! - `preflight` (#32): validates DB/NAS/Discord connectivity.
//! - `summary` (#33): consolidates config loading (#30), DB schema
//!   verification (#31) and the preflight checks above into one
//!   human-readable startup summary, orchestrated from
//!   `main.rs::run_master`.
pub mod preflight;
pub mod summary;

pub use preflight::{run_preflight_checks, CheckStatus, PreflightCheck, PreflightError};
pub use summary::{ApiInfo, DbInfo, DiscordInfo, NasInfo, StartupSummary};
