//! `trein-video` as a library: the `trein-video` binary (`src/main.rs`) is a
//! thin wrapper around this crate's modules, and integration tests under
//! `tests/` link against it directly to exercise the master API server, the
//! worker sync coordinator, and the local processing pipeline end-to-end
//! without going through a spawned subprocess (see #16).
pub mod api;
pub mod cli;
pub mod config;
pub mod db;
pub mod discord;
pub mod error;
pub mod nas;
pub mod progress;
pub mod sync;
pub mod tls;
pub mod utils;
pub mod video;
pub mod worker;
