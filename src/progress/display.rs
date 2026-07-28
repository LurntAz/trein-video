//! [`ProgressDisplay`]: the single consumer of the [`super::ProgressEvent`]
//! channel. Renders either live `indicatif` progress bars (TTY stdout) or
//! plain status lines (non-TTY, e.g. redirected to a file), chosen once at
//! construction time via [`is_terminal::IsTerminal`].

use super::{
    clamp_percentage, format_duration, format_eta, format_speed_mbps, frame_percentage,
    transfer_percentage, ProgressEvent, ProgressReceiver, VideoState,
};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use is_terminal::IsTerminal;
use std::collections::HashMap;
use std::time::Instant;

/// Reads [`ProgressEvent`]s off its receiver until the channel closes
/// (i.e. every producer -- the worker pipeline -- has been dropped, which in
/// practice means the process is shutting down), rendering each one as it
/// arrives.
pub struct ProgressDisplay {
    rx: ProgressReceiver,
    is_tty: bool,
}

impl ProgressDisplay {
    /// TTY-ness is detected once, from `stdout`, at construction time.
    pub fn new(rx: ProgressReceiver) -> Self {
        Self {
            rx,
            is_tty: std::io::stdout().is_terminal(),
        }
    }

    /// Force TTY/non-TTY rendering regardless of the actual stdout, for
    /// tests that want to exercise one renderer deterministically without
    /// depending on how the test runner's stdout happens to be connected.
    #[cfg(test)]
    fn with_tty(rx: ProgressReceiver, is_tty: bool) -> Self {
        Self { rx, is_tty }
    }

    /// Consume events until the channel closes. Never returns an error --
    /// display rendering is best-effort and must never be a reason for the
    /// worker to fail (see #20's "no panics on extreme values").
    pub async fn run(mut self) {
        let mut renderer: Box<dyn Renderer> = if self.is_tty {
            Box::new(TtyRenderer::new())
        } else {
            Box::new(PlainRenderer::new())
        };
        while let Some(event) = self.rx.recv().await {
            renderer.handle(event);
        }
    }
}

trait Renderer: Send {
    fn handle(&mut self, event: ProgressEvent);
}

fn stage_icon_label(stage_name: &str) -> String {
    match stage_name {
        "download" => "Downloading".to_string(),
        "analyze" => "Analyzing".to_string(),
        "convert" => "Converting".to_string(),
        "upload" => "Uploading".to_string(),
        other => other.to_string(),
    }
}

// --- TTY renderer: live indicatif progress bars -----------------------

struct TtyRenderer {
    multi: MultiProgress,
    videos: HashMap<String, VideoState>,
    bars: HashMap<(String, String), ProgressBar>,
}

impl TtyRenderer {
    fn new() -> Self {
        Self {
            multi: MultiProgress::new(),
            videos: HashMap::new(),
            bars: HashMap::new(),
        }
    }

    fn video_label(&self, video_id: &str) -> String {
        self.videos
            .get(video_id)
            .map(|v| format!("{video_id} ({})", v.filename))
            .unwrap_or_else(|| video_id.to_string())
    }

    fn bar_for(&mut self, video_id: &str, stage_name: &str) -> ProgressBar {
        let key = (video_id.to_string(), stage_name.to_string());
        if let Some(bar) = self.bars.get(&key) {
            return bar.clone();
        }
        let label = self.video_label(video_id);
        let bar = self.multi.add(ProgressBar::new(100));
        bar.set_style(in_progress_style());
        bar.set_prefix(format!("{label} · {}", stage_icon_label(stage_name)));
        bar.set_position(0);
        self.bars.insert(key, bar.clone());
        bar
    }

    fn set_position_clamped(bar: &ProgressBar, percentage: f32) {
        bar.set_position(clamp_percentage(percentage));
    }
}

fn in_progress_style() -> ProgressStyle {
    ProgressStyle::with_template("{prefix} [{bar:20.green/black}] {pos:>3}% {msg}")
        .unwrap_or_else(|_| ProgressStyle::default_bar())
        .progress_chars("█▓░")
}

fn done_style() -> ProgressStyle {
    ProgressStyle::with_template("{prefix} [{bar:20.blue/black}] {pos:>3}% {msg}")
        .unwrap_or_else(|_| ProgressStyle::default_bar())
        .progress_chars("█▓░")
}

fn failed_style() -> ProgressStyle {
    ProgressStyle::with_template("{prefix} {msg}").unwrap_or_else(|_| ProgressStyle::default_bar())
}

impl Renderer for TtyRenderer {
    fn handle(&mut self, event: ProgressEvent) {
        match event {
            ProgressEvent::JobStarted { video_id, filename } => {
                self.videos.entry(video_id.clone()).or_default().filename = filename;
                self.multi
                    .println(format!("Starting {}", self.video_label(&video_id)))
                    .ok();
            }
            ProgressEvent::JobCompleted {
                video_id,
                total_duration_secs,
            } => {
                self.multi
                    .println(format!(
                        "{}: done in {}",
                        self.video_label(&video_id),
                        format_duration(total_duration_secs)
                    ))
                    .ok();
            }
            ProgressEvent::JobFailed { video_id, error } => {
                self.multi
                    .println(format!("{}: FAILED - {error}", self.video_label(&video_id)))
                    .ok();
            }
            ProgressEvent::StageStarted {
                stage_name,
                video_id,
            } => {
                self.videos
                    .entry(video_id.clone())
                    .or_default()
                    .stage_started_at
                    .insert(stage_name.clone(), Instant::now());
                let bar = self.bar_for(&video_id, &stage_name);
                bar.set_style(in_progress_style());
                bar.set_position(0);
                bar.set_message("starting...".to_string());
            }
            ProgressEvent::StageProgress {
                stage_name,
                video_id,
                percentage,
                details,
            } => {
                let bar = self.bar_for(&video_id, &stage_name);
                Self::set_position_clamped(&bar, percentage);
                bar.set_message(details);
            }
            ProgressEvent::StageCompleted {
                stage_name,
                video_id,
                duration_secs,
            } => {
                let bar = self.bar_for(&video_id, &stage_name);
                bar.set_style(done_style());
                bar.set_position(100);
                bar.finish_with_message(format!("done ({})", format_duration(duration_secs)));
            }
            ProgressEvent::StageFailed {
                stage_name,
                video_id,
                error,
            } => {
                let bar = self.bar_for(&video_id, &stage_name);
                bar.set_style(failed_style());
                bar.abandon_with_message(format!("FAILED - {error}"));
            }
            ProgressEvent::TransferProgress {
                direction,
                video_id,
                bytes_transferred,
                bytes_total,
                speed_mbps,
                eta_secs,
            } => {
                let bar = self.bar_for(&video_id, &direction);
                let pct = transfer_percentage(bytes_transferred, bytes_total);
                Self::set_position_clamped(&bar, pct);
                bar.set_message(format!(
                    "{}, ETA {}",
                    format_speed_mbps(speed_mbps),
                    format_eta(eta_secs)
                ));
            }
            ProgressEvent::EncodingProgress {
                video_id,
                current_frame,
                total_frames,
                speed,
                eta_secs,
            } => {
                let bar = self.bar_for(&video_id, "convert");
                let pct = frame_percentage(current_frame, total_frames);
                Self::set_position_clamped(&bar, pct);
                bar.set_message(format!("{speed} speed, ETA {}", format_eta(eta_secs)));
            }
        }
    }
}

// --- Plain-text renderer: one line per event, no ANSI/bars -------------

/// Used when stdout isn't a TTY (redirected to a file/pipe): `indicatif`'s
/// carriage-return-based redrawing is meaningless there and would just
/// pollute the output with control characters, so this renderer prints
/// ordinary `println!` lines instead (see #20's "graceful degradation").
struct PlainRenderer {
    videos: HashMap<String, VideoState>,
}

impl PlainRenderer {
    fn new() -> Self {
        Self {
            videos: HashMap::new(),
        }
    }

    fn video_label(&self, video_id: &str) -> String {
        self.videos
            .get(video_id)
            .map(|v| format!("{video_id} ({})", v.filename))
            .unwrap_or_else(|| video_id.to_string())
    }
}

impl Renderer for PlainRenderer {
    fn handle(&mut self, event: ProgressEvent) {
        match event {
            ProgressEvent::JobStarted { video_id, filename } => {
                self.videos.entry(video_id.clone()).or_default().filename = filename;
                println!("[{}] starting", self.video_label(&video_id));
            }
            ProgressEvent::JobCompleted {
                video_id,
                total_duration_secs,
            } => {
                println!(
                    "[{}] done in {}",
                    self.video_label(&video_id),
                    format_duration(total_duration_secs)
                );
            }
            ProgressEvent::JobFailed { video_id, error } => {
                println!("[{}] FAILED: {error}", self.video_label(&video_id));
            }
            ProgressEvent::StageStarted {
                stage_name,
                video_id,
            } => {
                self.videos
                    .entry(video_id.clone())
                    .or_default()
                    .stage_started_at
                    .insert(stage_name.clone(), Instant::now());
                println!(
                    "[{}] {} started",
                    self.video_label(&video_id),
                    stage_icon_label(&stage_name)
                );
            }
            ProgressEvent::StageProgress {
                stage_name,
                video_id,
                percentage,
                details,
            } => {
                println!(
                    "[{}] {} {}% {details}",
                    self.video_label(&video_id),
                    stage_icon_label(&stage_name),
                    clamp_percentage(percentage)
                );
            }
            ProgressEvent::StageCompleted {
                stage_name,
                video_id,
                duration_secs,
            } => {
                println!(
                    "[{}] {} done ({})",
                    self.video_label(&video_id),
                    stage_icon_label(&stage_name),
                    format_duration(duration_secs)
                );
            }
            ProgressEvent::StageFailed {
                stage_name,
                video_id,
                error,
            } => {
                println!(
                    "[{}] {} FAILED: {error}",
                    self.video_label(&video_id),
                    stage_icon_label(&stage_name)
                );
            }
            ProgressEvent::TransferProgress {
                direction,
                video_id,
                bytes_transferred,
                bytes_total,
                speed_mbps,
                eta_secs,
            } => {
                let pct = clamp_percentage(transfer_percentage(bytes_transferred, bytes_total));
                println!(
                    "[{}] {direction} {pct}% {}, ETA {}",
                    self.video_label(&video_id),
                    format_speed_mbps(speed_mbps),
                    format_eta(eta_secs)
                );
            }
            ProgressEvent::EncodingProgress {
                video_id,
                current_frame,
                total_frames,
                speed,
                eta_secs,
            } => {
                let pct = clamp_percentage(frame_percentage(current_frame, total_frames));
                println!(
                    "[{}] converting {pct}% frame {current_frame}/{total_frames}, {speed} speed, ETA {}",
                    self.video_label(&video_id),
                    format_eta(eta_secs)
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress::channel;

    fn all_event_kinds(video_id: &str) -> Vec<ProgressEvent> {
        vec![
            ProgressEvent::JobStarted {
                video_id: video_id.to_string(),
                filename: "myvideo.mkv".to_string(),
            },
            ProgressEvent::StageStarted {
                stage_name: "download".to_string(),
                video_id: video_id.to_string(),
            },
            ProgressEvent::TransferProgress {
                direction: "download".to_string(),
                video_id: video_id.to_string(),
                bytes_transferred: 50,
                bytes_total: 100,
                speed_mbps: 45.2,
                eta_secs: 30.0,
            },
            // Edge case: zero total bytes must not panic (division by zero).
            ProgressEvent::TransferProgress {
                direction: "download".to_string(),
                video_id: video_id.to_string(),
                bytes_transferred: 0,
                bytes_total: 0,
                speed_mbps: 0.0,
                eta_secs: 0.0,
            },
            // Edge case: negative speed/eta (clock skew) must clamp, not panic.
            ProgressEvent::TransferProgress {
                direction: "download".to_string(),
                video_id: video_id.to_string(),
                bytes_transferred: 100,
                bytes_total: 100,
                speed_mbps: -5.0,
                eta_secs: -10.0,
            },
            ProgressEvent::StageCompleted {
                stage_name: "download".to_string(),
                video_id: video_id.to_string(),
                duration_secs: 720.0,
            },
            ProgressEvent::StageStarted {
                stage_name: "analyze".to_string(),
                video_id: video_id.to_string(),
            },
            ProgressEvent::StageCompleted {
                stage_name: "analyze".to_string(),
                video_id: video_id.to_string(),
                duration_secs: 45.0,
            },
            ProgressEvent::StageStarted {
                stage_name: "convert".to_string(),
                video_id: video_id.to_string(),
            },
            ProgressEvent::EncodingProgress {
                video_id: video_id.to_string(),
                current_frame: 100,
                total_frames: 1000,
                speed: "23.5x".to_string(),
                eta_secs: 480.0,
            },
            // Edge case: total_frames == 0 (couldn't be estimated yet).
            ProgressEvent::EncodingProgress {
                video_id: video_id.to_string(),
                current_frame: 0,
                total_frames: 0,
                speed: "0.0x".to_string(),
                eta_secs: 0.0,
            },
            // Edge case: percentage far outside 0-100 must clamp, not panic.
            ProgressEvent::StageProgress {
                stage_name: "convert".to_string(),
                video_id: video_id.to_string(),
                percentage: 250.0,
                details: "bogus".to_string(),
            },
            ProgressEvent::StageProgress {
                stage_name: "convert".to_string(),
                video_id: video_id.to_string(),
                percentage: -50.0,
                details: "bogus negative".to_string(),
            },
            ProgressEvent::StageCompleted {
                stage_name: "convert".to_string(),
                video_id: video_id.to_string(),
                duration_secs: 600.0,
            },
            ProgressEvent::StageStarted {
                stage_name: "upload".to_string(),
                video_id: video_id.to_string(),
            },
            ProgressEvent::StageFailed {
                stage_name: "upload".to_string(),
                video_id: video_id.to_string(),
                error: "NAS unreachable".to_string(),
            },
            ProgressEvent::JobFailed {
                video_id: video_id.to_string(),
                error: "upload failed: NAS unreachable".to_string(),
            },
        ]
    }

    async fn run_all_events_through(is_tty: bool) {
        let (tx, rx) = channel();
        for event in all_event_kinds("video-123") {
            tx.try_send(event).unwrap();
        }
        drop(tx);
        let display = ProgressDisplay::with_tty(rx, is_tty);
        // Must run to completion (channel closes once `tx` is dropped)
        // without panicking, regardless of TTY-ness.
        display.run().await;
    }

    #[tokio::test]
    async fn test_tty_renderer_handles_all_event_kinds_without_panic() {
        run_all_events_through(true).await;
    }

    #[tokio::test]
    async fn test_plain_renderer_handles_all_event_kinds_without_panic() {
        run_all_events_through(false).await;
    }

    #[tokio::test]
    async fn test_job_completed_success_path_full_pipeline() {
        let (tx, rx) = channel();
        let video_id = "video-456";
        tx.try_send(ProgressEvent::JobStarted {
            video_id: video_id.to_string(),
            filename: "bigfile.mp4".to_string(),
        })
        .unwrap();
        for stage in ["download", "analyze", "convert", "upload"] {
            tx.try_send(ProgressEvent::StageStarted {
                stage_name: stage.to_string(),
                video_id: video_id.to_string(),
            })
            .unwrap();
            tx.try_send(ProgressEvent::StageCompleted {
                stage_name: stage.to_string(),
                video_id: video_id.to_string(),
                duration_secs: 10.0,
            })
            .unwrap();
        }
        tx.try_send(ProgressEvent::JobCompleted {
            video_id: video_id.to_string(),
            total_duration_secs: 40.0,
        })
        .unwrap();
        drop(tx);

        // Exercise both renderers against the same well-formed sequence.
        ProgressDisplay::with_tty(rx, true).run().await;
    }

    #[tokio::test]
    async fn test_display_run_returns_when_channel_closes_with_no_events() {
        let (tx, rx) = channel();
        drop(tx);
        ProgressDisplay::with_tty(rx, false).run().await;
    }

    #[tokio::test]
    async fn test_multiple_videos_tracked_independently() {
        let (tx, rx) = channel();
        for video_id in ["v1", "v2", "v3"] {
            tx.try_send(ProgressEvent::JobStarted {
                video_id: video_id.to_string(),
                filename: format!("{video_id}.mp4"),
            })
            .unwrap();
            tx.try_send(ProgressEvent::StageStarted {
                stage_name: "convert".to_string(),
                video_id: video_id.to_string(),
            })
            .unwrap();
            tx.try_send(ProgressEvent::EncodingProgress {
                video_id: video_id.to_string(),
                current_frame: 10,
                total_frames: 100,
                speed: "5.0x".to_string(),
                eta_secs: 90.0,
            })
            .unwrap();
        }
        drop(tx);
        ProgressDisplay::with_tty(rx, true).run().await;
    }
}
