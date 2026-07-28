use super::optimizer::EncodingParams;
use crate::progress::{send_progress, ProgressEvent, ProgressSender};
use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tracing::{debug, info, instrument};

/// Minimum time between two `EncodingProgress` events for the same encode --
/// ffmpeg can print a stderr progress line many times per second on a fast
/// encode, and emitting an event for every single one would just spam the
/// (bounded, fire-and-forget) progress channel for no visible benefit to a
/// human watching a progress bar (see #20's plan: "emit periodically").
const ENCODING_PROGRESS_MIN_INTERVAL: Duration = Duration::from_millis(500);

/// Context needed to turn ffmpeg's stderr progress lines into
/// [`ProgressEvent::EncodingProgress`] events for one specific encode.
struct EncodingProgressContext {
    video_id: String,
    /// Total source duration (from `ffprobe`, #6), used together with a
    /// parsed `fps=` sample to estimate `total_frames` -- ffmpeg's own
    /// stderr progress output doesn't print an upfront total frame count.
    duration_secs: f64,
    tx: ProgressSender,
}

/// One parsed sample from an ffmpeg stderr progress line, e.g.:
/// `frame= 1234 fps=25 q=28.0 size=   10240kB time=00:01:23.45 bitrate=987.6kbits/s speed=23.5x`
///
/// Any subset of fields may be absent (ffmpeg's exact line format varies by
/// version/codec), so every field is optional; callers combine whatever was
/// found with best-effort fallbacks.
#[derive(Debug, Default, PartialEq)]
struct FfmpegProgressSample {
    frame: Option<u64>,
    fps: Option<f64>,
    speed: Option<f64>,
}

/// Parse a single ffmpeg stderr line for `frame=`, `fps=`, and `speed=`
/// key/value pairs. Returns `None` if the line doesn't look like a progress
/// line at all (no `frame=` token), so ordinary log lines (warnings,
/// codec info, ...) are cheaply skipped.
fn parse_ffmpeg_progress_line(line: &str) -> Option<FfmpegProgressSample> {
    if !line.contains("frame=") {
        return None;
    }
    let frame = extract_ffmpeg_field(line, "frame=").and_then(|v| v.parse::<u64>().ok());
    let fps = extract_ffmpeg_field(line, "fps=").and_then(|v| v.parse::<f64>().ok());
    let speed = extract_ffmpeg_field(line, "speed=")
        .map(|v| v.trim_end_matches('x').to_string())
        .and_then(|v| v.parse::<f64>().ok());
    Some(FfmpegProgressSample { frame, fps, speed })
}

/// Extract the value following `key` (e.g. `"frame="`) up to the next
/// whitespace, tolerating the extra spaces ffmpeg pads its progress fields
/// with (e.g. `"frame=  1234 "`).
fn extract_ffmpeg_field<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let start = line.find(key)? + key.len();
    let rest = line[start..].trim_start();
    let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
    let value = &rest[..end];
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

#[derive(Debug, Error)]
pub enum ConverterError {
    #[error("ffmpeg binary not found in PATH")]
    BinaryNotFound,
    #[error("invalid input file: {0}")]
    InvalidInput(String),
    #[error("ffmpeg encoding failed (exit code {0:?}): {1}")]
    EncodingFailed(Option<i32>, String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Parse one ffmpeg stderr `line`, and if it's a progress line and enough
/// time has passed since `last_emitted` (throttled per
/// [`ENCODING_PROGRESS_MIN_INTERVAL`], updated in place), send an
/// [`ProgressEvent::EncodingProgress`] on `ctx.tx`.
///
/// `total_frames` is *estimated* as `duration_secs * fps` from the current
/// sample's `fps=` field, since ffmpeg's stderr progress output never prints
/// an upfront total frame count -- this is inherently approximate (fps
/// fluctuates during encoding) but good enough for a progress bar/ETA.
fn maybe_emit_encoding_progress(
    line: &str,
    ctx: &EncodingProgressContext,
    last_emitted: &mut Option<Instant>,
) {
    let Some(sample) = parse_ffmpeg_progress_line(line) else {
        return;
    };
    let now = Instant::now();
    if let Some(last) = last_emitted {
        if now.duration_since(*last) < ENCODING_PROGRESS_MIN_INTERVAL {
            return;
        }
    }
    *last_emitted = Some(now);

    let current_frame = sample.frame.unwrap_or(0);
    let fps = sample.fps.filter(|f| *f > 0.0);
    let total_frames = match fps {
        Some(fps) if ctx.duration_secs > 0.0 => (ctx.duration_secs * fps).round() as u64,
        _ => 0,
    };
    let eta_secs = match fps {
        Some(fps) if total_frames > current_frame => {
            (total_frames - current_frame) as f32 / fps as f32
        }
        _ => 0.0,
    };
    let speed = sample
        .speed
        .map(|s| format!("{s:.1}x"))
        .unwrap_or_else(|| "?x".to_string());

    send_progress(
        &ctx.tx,
        ProgressEvent::EncodingProgress {
            video_id: ctx.video_id.clone(),
            current_frame,
            total_frames,
            speed,
            eta_secs,
        },
    );
}

pub struct VideoConverter {
    preset: String,
    crf: u8,
}

impl VideoConverter {
    pub fn new(preset: String, crf: u8) -> Self {
        Self { preset, crf }
    }

    /// Encode `input_path` to AV1 (`libsvtav1`) at `output_path`, returning
    /// the size in bytes of the produced file.
    #[instrument(skip(self), fields(preset = %self.preset, crf = self.crf))]
    pub async fn convert_to_av1<P: AsRef<Path> + std::fmt::Debug>(
        &self,
        input_path: P,
        output_path: P,
    ) -> Result<u64, ConverterError> {
        self.run_ffmpeg(input_path.as_ref(), output_path.as_ref(), "libsvtav1")
            .await
    }

    /// Encode `input_path` to H.265 (`libx265`) at `output_path`, returning
    /// the size in bytes of the produced file.
    #[instrument(skip(self), fields(preset = %self.preset, crf = self.crf))]
    pub async fn convert_to_h265<P: AsRef<Path> + std::fmt::Debug>(
        &self,
        input_path: P,
        output_path: P,
    ) -> Result<u64, ConverterError> {
        self.run_ffmpeg(input_path.as_ref(), output_path.as_ref(), "libx265")
            .await
    }

    /// Encode `input_path` at `output_path` using `video_codec` (e.g.
    /// `"libsvtav1"`/`"libx265"`) with explicit, per-file `params` computed
    /// by [`crate::video::EncodingOptimizer`] rather than this converter's
    /// static defaults (see #19). `convert_to_av1`/`convert_to_h265` remain
    /// the simple, config-driven entry points for callers without
    /// content-aware parameters (e.g. direct unit/integration tests).
    #[instrument(skip(self, params), fields(preset = %params.preset, crf = params.crf))]
    pub async fn convert_with_params<P: AsRef<Path> + std::fmt::Debug>(
        &self,
        input_path: P,
        output_path: P,
        video_codec: &str,
        params: &EncodingParams,
    ) -> Result<u64, ConverterError> {
        self.run_ffmpeg_impl(
            input_path.as_ref(),
            output_path.as_ref(),
            video_codec,
            &params.preset,
            params.crf,
            &params.extra_args,
            None,
        )
        .await
    }

    /// Same as [`Self::convert_with_params`], additionally emitting
    /// [`ProgressEvent::EncodingProgress`] events (#20) as ffmpeg reports
    /// progress on stderr, so callers with a source `duration_secs` (#6) and
    /// a place to send progress to (the worker pipeline, #11) get live
    /// feedback for what's typically the longest-running stage.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(self, params, progress_tx), fields(preset = %params.preset, crf = params.crf))]
    pub async fn convert_with_progress<P: AsRef<Path> + std::fmt::Debug>(
        &self,
        input_path: P,
        output_path: P,
        video_codec: &str,
        params: &EncodingParams,
        video_id: &str,
        duration_secs: f64,
        progress_tx: &ProgressSender,
    ) -> Result<u64, ConverterError> {
        self.run_ffmpeg_impl(
            input_path.as_ref(),
            output_path.as_ref(),
            video_codec,
            &params.preset,
            params.crf,
            &params.extra_args,
            Some(EncodingProgressContext {
                video_id: video_id.to_string(),
                duration_secs,
                tx: progress_tx.clone(),
            }),
        )
        .await
    }

    async fn run_ffmpeg(
        &self,
        input_path: &Path,
        output_path: &Path,
        video_codec: &str,
    ) -> Result<u64, ConverterError> {
        self.run_ffmpeg_impl(
            input_path,
            output_path,
            video_codec,
            &self.preset,
            self.crf,
            &[],
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_ffmpeg_impl(
        &self,
        input_path: &Path,
        output_path: &Path,
        video_codec: &str,
        preset: &str,
        crf: u8,
        extra_args: &[String],
        progress: Option<EncodingProgressContext>,
    ) -> Result<u64, ConverterError> {
        if !input_path.exists() {
            return Err(ConverterError::InvalidInput(format!(
                "{} does not exist",
                input_path.display()
            )));
        }

        if let Some(parent) = output_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut cmd = Command::new("ffmpeg");
        cmd.arg("-y") // overwrite output if it exists (job retried after crash)
            .arg("-i")
            .arg(input_path)
            .args(["-c:v", video_codec])
            .args(["-preset", preset])
            .args(["-crf", &crf.to_string()])
            .args(extra_args)
            .args(["-c:a", "copy"])
            .arg(output_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());

        info!(
            input = %input_path.display(),
            output = %output_path.display(),
            codec = video_codec,
            "spawning ffmpeg"
        );

        let mut child = cmd.spawn().map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                ConverterError::BinaryNotFound
            } else {
                ConverterError::Io(e)
            }
        })?;

        let stderr = child.stderr.take().expect("stderr was configured as piped");
        let mut lines = BufReader::new(stderr).lines();

        // Drain + capture ffmpeg's stderr as it comes in, at `debug` level
        // (ffmpeg progress output is far too noisy for `info`), additionally
        // parsing progress (`frame=`/`fps=`/`speed=`) out of it when a
        // `progress` context was given (#20).
        let stderr_task = tokio::spawn(async move {
            let mut collected = String::new();
            let mut last_emitted: Option<Instant> = None;
            while let Ok(Some(line)) = lines.next_line().await {
                debug!(target: "ffmpeg", "{line}");
                if let Some(ctx) = &progress {
                    maybe_emit_encoding_progress(&line, ctx, &mut last_emitted);
                }
                collected.push_str(&line);
                collected.push('\n');
            }
            collected
        });

        let status = child.wait().await?;

        let stderr_output = stderr_task.await.unwrap_or_default();

        if !status.success() {
            // Never leave a truncated/corrupt file behind for the uploader to pick up.
            let _ = tokio::fs::remove_file(output_path).await;
            return Err(ConverterError::EncodingFailed(status.code(), stderr_output));
        }

        let metadata = tokio::fs::metadata(output_path).await?;
        info!(
            output = %output_path.display(),
            size_bytes = metadata.len(),
            "ffmpeg encoding finished"
        );
        Ok(metadata.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress::channel;

    #[test]
    fn test_converter_creation() {
        let converter = VideoConverter::new("slow".to_string(), 32);
        assert_eq!(converter.preset, "slow");
        assert_eq!(converter.crf, 32);
    }

    // --- #20: ffmpeg stderr progress-line parsing ---

    #[test]
    fn test_parse_ffmpeg_progress_line_full_line() {
        let line = "frame= 1234 fps=25 q=28.0 size=   10240kB time=00:01:23.45 bitrate= 987.6kbits/s speed=23.5x";
        let sample = parse_ffmpeg_progress_line(line).unwrap();
        assert_eq!(sample.frame, Some(1234));
        assert_eq!(sample.fps, Some(25.0));
        assert_eq!(sample.speed, Some(23.5));
    }

    #[test]
    fn test_parse_ffmpeg_progress_line_non_progress_line_returns_none() {
        assert_eq!(
            parse_ffmpeg_progress_line("Stream #0:0: Video: h264, yuv420p, 1920x1080"),
            None
        );
    }

    #[test]
    fn test_parse_ffmpeg_progress_line_missing_fields_are_none() {
        let sample = parse_ffmpeg_progress_line("frame=  100").unwrap();
        assert_eq!(sample.frame, Some(100));
        assert_eq!(sample.fps, None);
        assert_eq!(sample.speed, None);
    }

    #[test]
    fn test_parse_ffmpeg_progress_line_slow_speed_under_1x() {
        let sample = parse_ffmpeg_progress_line("frame=10 fps=2 speed=0.8x").unwrap();
        assert_eq!(sample.speed, Some(0.8));
    }

    #[test]
    fn test_maybe_emit_encoding_progress_computes_eta_and_total_frames() {
        let (tx, mut rx) = channel();
        let ctx = EncodingProgressContext {
            video_id: "v1".to_string(),
            duration_secs: 100.0, // at fps=10 => 1000 total frames
            tx,
        };
        let mut last_emitted = None;
        maybe_emit_encoding_progress("frame=100 fps=10 speed=5.0x", &ctx, &mut last_emitted);
        let event = rx.try_recv().unwrap();
        match event {
            ProgressEvent::EncodingProgress {
                video_id,
                current_frame,
                total_frames,
                speed,
                eta_secs,
            } => {
                assert_eq!(video_id, "v1");
                assert_eq!(current_frame, 100);
                assert_eq!(total_frames, 1000);
                assert_eq!(speed, "5.0x");
                // (1000 - 100) / 10 fps = 90s remaining.
                assert!((eta_secs - 90.0).abs() < 0.01);
            }
            other => panic!("unexpected event: {other:?}"),
        }
        assert!(last_emitted.is_some());
    }

    #[test]
    fn test_maybe_emit_encoding_progress_throttles_rapid_updates() {
        let (tx, mut rx) = channel();
        let ctx = EncodingProgressContext {
            video_id: "v1".to_string(),
            duration_secs: 100.0,
            tx,
        };
        let mut last_emitted = Some(Instant::now());
        maybe_emit_encoding_progress("frame=200 fps=10 speed=5.0x", &ctx, &mut last_emitted);
        // Immediately after a just-recorded emission, the throttle should
        // suppress this one.
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_maybe_emit_encoding_progress_zero_fps_yields_zero_total_frames_no_panic() {
        let (tx, mut rx) = channel();
        let ctx = EncodingProgressContext {
            video_id: "v1".to_string(),
            duration_secs: 100.0,
            tx,
        };
        let mut last_emitted = None;
        maybe_emit_encoding_progress("frame=10 fps=0 speed=0.0x", &ctx, &mut last_emitted);
        let event = rx.try_recv().unwrap();
        match event {
            ProgressEvent::EncodingProgress {
                total_frames,
                eta_secs,
                ..
            } => {
                assert_eq!(total_frames, 0);
                assert_eq!(eta_secs, 0.0);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn test_maybe_emit_encoding_progress_non_progress_line_is_noop() {
        let (tx, mut rx) = channel();
        let ctx = EncodingProgressContext {
            video_id: "v1".to_string(),
            duration_secs: 100.0,
            tx,
        };
        let mut last_emitted = None;
        maybe_emit_encoding_progress("some unrelated log line", &ctx, &mut last_emitted);
        assert!(rx.try_recv().is_err());
        assert!(last_emitted.is_none());
    }

    #[tokio::test]
    async fn test_convert_missing_input_returns_invalid_input() {
        let converter = VideoConverter::new("fast".to_string(), 32);
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("missing.mp4");
        let output = dir.path().join("out.mkv");

        let result = converter.convert_to_av1(input, output).await;
        assert!(matches!(result, Err(ConverterError::InvalidInput(_))));
    }

    #[tokio::test]
    async fn test_convert_with_params_missing_input_returns_invalid_input() {
        let converter = VideoConverter::new("fast".to_string(), 32);
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("missing.mp4");
        let output = dir.path().join("out.mkv");
        let params = EncodingParams {
            preset: "medium".to_string(),
            crf: 30,
            threads_per_job: 2,
            extra_args: vec!["-svtav1-params".to_string(), "lp=2".to_string()],
        };

        let result = converter
            .convert_with_params(input, output, "libsvtav1", &params)
            .await;
        assert!(matches!(result, Err(ConverterError::InvalidInput(_))));
    }

    #[tokio::test]
    async fn test_convert_with_progress_missing_input_returns_invalid_input() {
        let converter = VideoConverter::new("fast".to_string(), 32);
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("missing.mp4");
        let output = dir.path().join("out.mkv");
        let params = EncodingParams {
            preset: "medium".to_string(),
            crf: 30,
            threads_per_job: 2,
            extra_args: vec![],
        };
        let (tx, _rx) = channel();

        let result = converter
            .convert_with_progress(input, output, "libsvtav1", &params, "video-1", 60.0, &tx)
            .await;
        assert!(matches!(result, Err(ConverterError::InvalidInput(_))));
    }

    /// Requires a real `ffmpeg` on PATH with `libsvtav1`/`libx265` support.
    /// Run explicitly with `cargo test -- --ignored`.
    #[tokio::test]
    #[ignore]
    async fn test_convert_to_av1_real_file() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.mp4");
        let output = dir.path().join("out.mkv");

        let status = tokio::process::Command::new("ffmpeg")
            .args(["-f", "lavfi", "-i", "testsrc=duration=1:size=64x64:rate=1"])
            .args(["-c:v", "libx264", "-y"])
            .arg(&input)
            .status()
            .await
            .expect("ffmpeg must be installed to run this test");
        assert!(status.success());

        let converter = VideoConverter::new("ultrafast".to_string(), 40);
        let size = converter
            .convert_to_av1(input, output.clone())
            .await
            .unwrap();
        assert!(size > 0);
        assert!(output.exists());
    }

    #[tokio::test]
    #[ignore]
    async fn test_convert_failure_cleans_up_output() {
        // A garbage input that ffprobe/ffmpeg cannot decode should fail
        // encoding and must not leave a truncated file behind.
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("garbage.mp4");
        tokio::fs::write(&input, b"not a real video file")
            .await
            .unwrap();
        let output = dir.path().join("out.mkv");

        let converter = VideoConverter::new("ultrafast".to_string(), 40);
        let result = converter.convert_to_av1(input, output.clone()).await;
        assert!(result.is_err());
        assert!(!output.exists());
    }
}
