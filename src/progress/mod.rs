//! Real-time progress tracking and console display for the download ->
//! analyze -> convert -> upload pipeline (#20).
//!
//! Producers (the worker pipeline: [`crate::worker::processor`],
//! [`crate::video::VideoConverter`], [`crate::worker::downloader`], ...) push
//! [`ProgressEvent`]s onto a bounded `mpsc` channel, fire-and-forget (see
//! [`send_progress`] -- it never blocks the pipeline, even if the consumer is
//! slow or absent). [`ProgressDisplay`] is the single consumer: it owns the
//! receiving end, maintains per-video/per-stage state, and renders it either
//! as live `indicatif` progress bars (TTY stdout) or as plain status lines
//! (redirected/non-TTY output, e.g. `trein-video ... > log.txt`).
//!
//! Stage lifecycle events (`StageStarted`/`StageCompleted`/`StageFailed`) are
//! emitted uniformly by [`crate::worker::processor::ProcessorOrchestrator`]
//! for all four stages (download/analyze/convert/upload), since it's the one
//! place that already knows when each stage starts and ends and how it
//! failed. Fine-grained *within-stage* progress -- which only the stage's own
//! implementation has visibility into -- is emitted by that implementation:
//! [`crate::worker::downloader`] emits `TransferProgress` for downloads (real
//! byte-level progress, polled from the partially-downloaded file on disk),
//! [`crate::video::converter`] emits `EncodingProgress` (parsed from ffmpeg's
//! stderr), and [`crate::worker::uploader`] emits a coarse start/complete
//! `TransferProgress` pair for uploads (no live byte-level signal is
//! available for an upload without extra round-trips to the NAS to poll the
//! in-progress remote file's size, which was judged not worth the added
//! complexity/fragility for this ticket).

use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::debug;

mod display;

pub use display::ProgressDisplay;

/// Capacity of the channel producers push [`ProgressEvent`]s onto (see #20's
/// plan: "Progress events are fire-and-forget on a bounded channel (100
/// cap)"). A full channel means events are dropped (see [`send_progress`])
/// rather than the pipeline ever blocking on a slow/stalled display.
pub const CHANNEL_CAPACITY: usize = 100;

pub type ProgressSender = mpsc::Sender<ProgressEvent>;
pub type ProgressReceiver = mpsc::Receiver<ProgressEvent>;

/// Create the channel producers and [`ProgressDisplay`] share.
pub fn channel() -> (ProgressSender, ProgressReceiver) {
    mpsc::channel(CHANNEL_CAPACITY)
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProgressEvent {
    // Job lifecycle.
    JobStarted {
        video_id: String,
        filename: String,
    },
    JobCompleted {
        video_id: String,
        total_duration_secs: f64,
    },
    JobFailed {
        video_id: String,
        error: String,
    },

    // Stage progression.
    StageStarted {
        stage_name: String,
        video_id: String,
    },
    StageProgress {
        stage_name: String,
        video_id: String,
        /// 0-100. Values outside that range (e.g. a caller passing a raw
        /// ratio like 0.5 instead of 50.0) are clamped by the display, never
        /// trusted as-is.
        percentage: f32,
        /// Free-form human-readable detail, e.g. "45.2 MB/s", "2m 30s
        /// remaining".
        details: String,
    },
    StageCompleted {
        stage_name: String,
        video_id: String,
        duration_secs: f64,
    },
    StageFailed {
        stage_name: String,
        video_id: String,
        error: String,
    },

    // Transfer speed (download/upload).
    TransferProgress {
        /// `"download"` or `"upload"`.
        direction: String,
        video_id: String,
        bytes_transferred: u64,
        bytes_total: u64,
        speed_mbps: f32,
        eta_secs: f32,
    },

    // Encoding progress (ffmpeg).
    EncodingProgress {
        video_id: String,
        current_frame: u64,
        total_frames: u64,
        /// e.g. `"23.5x"`.
        speed: String,
        eta_secs: f32,
    },
}

/// Push `event` onto `tx` without ever blocking the caller -- pipeline
/// stages must never stall waiting on a slow or full progress channel (see
/// #20's plan: "fire-and-forget"). Silently drops the event if the channel
/// is momentarily full, or if nothing is consuming events at all (e.g. unit
/// tests that construct an orchestrator without spawning a
/// [`ProgressDisplay`]); progress reporting is best-effort and never
/// correctness-critical.
pub fn send_progress(tx: &ProgressSender, event: ProgressEvent) {
    if let Err(err) = tx.try_send(event) {
        match err {
            mpsc::error::TrySendError::Full(_) => {
                debug!("progress channel full, dropping progress event");
            }
            mpsc::error::TrySendError::Closed(_) => {
                // No display consuming events; expected in most unit tests.
            }
        }
    }
}

/// Clamp a raw percentage into `0..=100` and round to the nearest integer
/// for bar rendering, guarding against both out-of-range values (e.g. a
/// caller passing a ratio instead of a percentage) and NaN (treated as 0).
pub(crate) fn clamp_percentage(percentage: f32) -> u64 {
    if !percentage.is_finite() {
        return 0;
    }
    percentage.clamp(0.0, 100.0).round() as u64
}

/// Clamp a possibly-negative speed (e.g. from clock skew during an ETA
/// calculation) to zero, per #20's edge cases ("network loss: if speed
/// calculation goes negative, clamp to 0").
pub(crate) fn clamp_nonneg(value: f32) -> f32 {
    if !value.is_finite() || value < 0.0 {
        0.0
    } else {
        value
    }
}

/// `bytes_transferred / bytes_total * 100`, guarding the `bytes_total == 0`
/// edge case (unknown/zero total size) by returning 0 instead of NaN/Inf.
pub(crate) fn transfer_percentage(bytes_transferred: u64, bytes_total: u64) -> f32 {
    if bytes_total == 0 {
        return 0.0;
    }
    (bytes_transferred as f64 / bytes_total as f64 * 100.0) as f32
}

/// `current_frame / total_frames * 100`, guarding `total_frames == 0`
/// (unknown total, e.g. duration/fps couldn't be determined) the same way.
pub(crate) fn frame_percentage(current_frame: u64, total_frames: u64) -> f32 {
    transfer_percentage(current_frame, total_frames)
}

/// Format a speed in bytes/sec as e.g. `"45.2 MB/s"`. Negative/non-finite
/// input is clamped to 0 first (see [`clamp_nonneg`]).
pub(crate) fn format_speed_mbps(speed_mbps: f32) -> String {
    format!("{:.1} MB/s", clamp_nonneg(speed_mbps))
}

/// Format a duration in seconds as e.g. `"2m 30s"` / `"45s"`. Negative/non-
/// finite input is clamped to 0 first.
pub(crate) fn format_eta(eta_secs: f32) -> String {
    let secs = clamp_nonneg(eta_secs).round() as u64;
    let minutes = secs / 60;
    let remaining_secs = secs % 60;
    if minutes > 0 {
        format!("{minutes}m {remaining_secs}s")
    } else {
        format!("{remaining_secs}s")
    }
}

/// Format a wall-clock duration in seconds as e.g. `"12 min"` / `"45 sec"`,
/// used for completed-stage summaries (as opposed to [`format_eta`], used
/// for time-remaining estimates).
pub(crate) fn format_duration(duration_secs: f64) -> String {
    let secs = if duration_secs.is_finite() && duration_secs > 0.0 {
        duration_secs.round() as u64
    } else {
        0
    };
    if secs >= 60 {
        format!("{} min", secs / 60)
    } else {
        format!("{secs} sec")
    }
}

/// Per-video state tracked by both renderers (TTY and plain-text): the
/// display name and a start time per stage so a `StageCompleted` without an
/// explicit duration (or a plain-text renderer wanting elapsed time) can
/// still report something reasonable.
#[derive(Default)]
pub(crate) struct VideoState {
    pub filename: String,
    pub stage_started_at: HashMap<String, Instant>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_send_progress_does_not_block_or_panic_when_channel_full() {
        let (tx, _rx) = mpsc::channel(1);
        tx.try_send(ProgressEvent::JobStarted {
            video_id: "v1".to_string(),
            filename: "a.mp4".to_string(),
        })
        .unwrap();
        // Channel is now full; send_progress must drop the event, not panic
        // or block.
        send_progress(
            &tx,
            ProgressEvent::JobStarted {
                video_id: "v2".to_string(),
                filename: "b.mp4".to_string(),
            },
        );
    }

    #[test]
    fn test_send_progress_does_not_panic_when_receiver_dropped() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        send_progress(
            &tx,
            ProgressEvent::JobStarted {
                video_id: "v1".to_string(),
                filename: "a.mp4".to_string(),
            },
        );
    }

    #[test]
    fn test_clamp_percentage_in_range_unchanged() {
        assert_eq!(clamp_percentage(42.4), 42);
    }

    #[test]
    fn test_clamp_percentage_above_100_clamped() {
        assert_eq!(clamp_percentage(250.0), 100);
    }

    #[test]
    fn test_clamp_percentage_negative_clamped_to_zero() {
        assert_eq!(clamp_percentage(-50.0), 0);
    }

    #[test]
    fn test_clamp_percentage_nan_is_zero() {
        assert_eq!(clamp_percentage(f32::NAN), 0);
    }

    #[test]
    fn test_clamp_nonneg_negative_clamped() {
        assert_eq!(clamp_nonneg(-5.0), 0.0);
    }

    #[test]
    fn test_clamp_nonneg_positive_unchanged() {
        assert_eq!(clamp_nonneg(5.0), 5.0);
    }

    #[test]
    fn test_transfer_percentage_zero_total_is_zero_not_nan() {
        let pct = transfer_percentage(50, 0);
        assert_eq!(pct, 0.0);
        assert!(!pct.is_nan());
    }

    #[test]
    fn test_transfer_percentage_normal_case() {
        assert_eq!(transfer_percentage(50, 100), 50.0);
    }

    #[test]
    fn test_frame_percentage_zero_total_is_zero() {
        assert_eq!(frame_percentage(10, 0), 0.0);
    }

    #[test]
    fn test_format_speed_mbps_clamps_negative() {
        assert_eq!(format_speed_mbps(-5.0), "0.0 MB/s");
    }

    #[test]
    fn test_format_speed_mbps_formats_one_decimal() {
        assert_eq!(format_speed_mbps(45.24), "45.2 MB/s");
    }

    #[test]
    fn test_format_eta_seconds_only() {
        assert_eq!(format_eta(45.0), "45s");
    }

    #[test]
    fn test_format_eta_minutes_and_seconds() {
        assert_eq!(format_eta(150.0), "2m 30s");
    }

    #[test]
    fn test_format_eta_negative_clamped_to_zero() {
        assert_eq!(format_eta(-10.0), "0s");
    }

    #[test]
    fn test_format_duration_under_a_minute() {
        assert_eq!(format_duration(45.0), "45 sec");
    }

    #[test]
    fn test_format_duration_minutes() {
        assert_eq!(format_duration(720.0), "12 min");
    }

    #[test]
    fn test_format_duration_negative_or_nan_is_zero_sec() {
        assert_eq!(format_duration(-5.0), "0 sec");
        assert_eq!(format_duration(f64::NAN), "0 sec");
    }

    #[tokio::test]
    async fn test_channel_respects_capacity() {
        let (tx, mut rx) = channel();
        for i in 0..CHANNEL_CAPACITY {
            tx.try_send(ProgressEvent::JobStarted {
                video_id: format!("v{i}"),
                filename: "a.mp4".to_string(),
            })
            .unwrap();
        }
        // One more than capacity should fail rather than block.
        assert!(tx
            .try_send(ProgressEvent::JobStarted {
                video_id: "overflow".to_string(),
                filename: "a.mp4".to_string(),
            })
            .is_err());
        rx.close();
    }
}
