use super::models::VideoMetadata;
use serde::Deserialize;
use std::path::Path;
use thiserror::Error;
use tracing::{debug, instrument};

/// Errors that can occur while probing a video file with `ffprobe`.
#[derive(Debug, Error)]
pub enum AnalyzerError {
    #[error("ffprobe binary not found in PATH")]
    BinaryNotFound,
    #[error("input file not found: {0}")]
    InputNotFound(String),
    #[error("ffprobe exited with status {0:?}: {1}")]
    ProbeFailed(Option<i32>, String),
    #[error("failed to parse ffprobe output: {0}")]
    ParseError(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct VideoAnalyzer;

#[derive(Debug, Deserialize)]
struct FfprobeOutput {
    #[serde(default)]
    streams: Vec<FfprobeStream>,
    format: FfprobeFormat,
}

#[derive(Debug, Deserialize)]
struct FfprobeStream {
    codec_type: String,
    #[serde(default)]
    codec_name: Option<String>,
    #[serde(default)]
    width: Option<u32>,
    #[serde(default)]
    height: Option<u32>,
    #[serde(default)]
    bit_rate: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FfprobeFormat {
    #[serde(default)]
    duration: Option<String>,
    #[serde(default)]
    size: Option<String>,
    #[serde(default)]
    bit_rate: Option<String>,
}

/// Classify a resolution into the coarse buckets used elsewhere in the
/// codebase ("1080p", "4K", ...). Falls back to `"{h}p"` for uncommon sizes.
fn resolution_label(width: u32, height: u32) -> String {
    let shorter_side = width.min(height);
    match shorter_side {
        0 => "unknown".to_string(),
        1..=480 => "480p".to_string(),
        481..=720 => "720p".to_string(),
        721..=1080 => "1080p".to_string(),
        1081..=1440 => "1440p".to_string(),
        _ => "4K".to_string(),
    }
}

impl VideoAnalyzer {
    /// Run `ffprobe` on `path` and turn its JSON output into a [`VideoMetadata`].
    #[instrument(skip_all, fields(path = %path.as_ref().display()))]
    pub async fn analyze<P: AsRef<Path>>(path: P) -> Result<VideoMetadata, AnalyzerError> {
        let path_ref = path.as_ref();
        if !path_ref.exists() {
            return Err(AnalyzerError::InputNotFound(path_ref.display().to_string()));
        }

        let output = tokio::process::Command::new("ffprobe")
            .args(["-v", "quiet", "-print_format", "json"])
            .args(["-show_format", "-show_streams"])
            .arg(path_ref)
            .output()
            .await
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    AnalyzerError::BinaryNotFound
                } else {
                    AnalyzerError::Io(e)
                }
            })?;

        if !output.status.success() {
            return Err(AnalyzerError::ProbeFailed(
                output.status.code(),
                String::from_utf8_lossy(&output.stderr).to_string(),
            ));
        }

        debug!("ffprobe succeeded, parsing JSON output");

        let parsed: FfprobeOutput = serde_json::from_slice(&output.stdout)
            .map_err(|e| AnalyzerError::ParseError(e.to_string()))?;

        let video_stream = parsed
            .streams
            .iter()
            .find(|s| s.codec_type == "video")
            .ok_or_else(|| AnalyzerError::ParseError("no video stream found".to_string()))?;

        let codec = video_stream
            .codec_name
            .clone()
            .unwrap_or_else(|| "unknown".to_string());

        let resolution = match (video_stream.width, video_stream.height) {
            (Some(w), Some(h)) => resolution_label(w, h),
            _ => "unknown".to_string(),
        };

        let bitrate_kbps = video_stream
            .bit_rate
            .as_ref()
            .or(parsed.format.bit_rate.as_ref())
            .and_then(|b| b.parse::<u64>().ok())
            .map(|b| (b / 1000) as u32)
            .unwrap_or(0);

        let filesize_bytes = parsed
            .format
            .size
            .as_ref()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        let duration_secs = parsed
            .format
            .duration
            .as_ref()
            .and_then(|d| d.parse::<f64>().ok())
            .unwrap_or(0.0);

        Ok(VideoMetadata {
            codec,
            bitrate_kbps,
            resolution,
            filesize_bytes,
            duration_secs,
        })
    }

    pub fn should_convert(metadata: &VideoMetadata) -> bool {
        // Convertir si:
        // 1. Codec est H.264, MPEG4, ou autre (pas AV1)
        // 2. OU filesize > 2GB (archivage)
        // 3. OU bitrate > 5000 kbps @ 1080p
        // 4. OU resolution > 1080p (downscale 4K → 1080p)

        metadata.codec != "av1"
            || metadata.filesize_bytes > 2_000_000_000
            || (metadata.bitrate_kbps > 5000 && metadata.resolution == "1080p")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_convert_h264() {
        let metadata = VideoMetadata {
            codec: "h264".to_string(),
            bitrate_kbps: 3000,
            resolution: "1080p".to_string(),
            filesize_bytes: 1_000_000_000,
            duration_secs: 3600.0,
        };
        assert!(VideoAnalyzer::should_convert(&metadata));
    }

    #[test]
    fn test_should_convert_av1_large_file() {
        let metadata = VideoMetadata {
            codec: "av1".to_string(),
            bitrate_kbps: 3000,
            resolution: "1080p".to_string(),
            filesize_bytes: 3_000_000_000, // 3GB
            duration_secs: 3600.0,
        };
        assert!(VideoAnalyzer::should_convert(&metadata));
    }

    #[test]
    fn test_should_not_convert_av1_small() {
        let metadata = VideoMetadata {
            codec: "av1".to_string(),
            bitrate_kbps: 2000,
            resolution: "1080p".to_string(),
            filesize_bytes: 500_000_000,
            duration_secs: 3600.0,
        };
        assert!(!VideoAnalyzer::should_convert(&metadata));
    }

    #[test]
    fn test_resolution_label() {
        assert_eq!(resolution_label(1920, 1080), "1080p");
        assert_eq!(resolution_label(3840, 2160), "4K");
        assert_eq!(resolution_label(1280, 720), "720p");
        assert_eq!(resolution_label(640, 480), "480p");
        assert_eq!(resolution_label(0, 0), "unknown");
    }

    #[test]
    fn test_parse_ffprobe_json() {
        let json = r#"{
            "streams": [
                {"codec_type": "video", "codec_name": "h264", "width": 1920, "height": 1080, "bit_rate": "5000000"},
                {"codec_type": "audio", "codec_name": "aac"}
            ],
            "format": {"duration": "120.5", "size": "123456789", "bit_rate": "5100000"}
        }"#;
        let parsed: FfprobeOutput = serde_json::from_str(json).unwrap();
        let video_stream = parsed
            .streams
            .iter()
            .find(|s| s.codec_type == "video")
            .unwrap();
        assert_eq!(video_stream.codec_name.as_deref(), Some("h264"));
        assert_eq!(parsed.format.duration.as_deref(), Some("120.5"));
    }

    #[tokio::test]
    async fn test_analyze_missing_file_returns_not_found() {
        let result = VideoAnalyzer::analyze("/nonexistent/path/to/video.mp4").await;
        assert!(matches!(result, Err(AnalyzerError::InputNotFound(_))));
    }

    /// Requires `ffmpeg`/`ffprobe` on PATH. Run explicitly with
    /// `cargo test -- --ignored` on a machine that has them installed.
    #[tokio::test]
    #[ignore]
    async fn test_analyze_real_file() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("test.mp4");

        let status = tokio::process::Command::new("ffmpeg")
            .args(["-f", "lavfi", "-i", "testsrc=duration=1:size=64x64:rate=1"])
            .args(["-c:v", "libx264", "-y"])
            .arg(&input)
            .status()
            .await
            .expect("ffmpeg must be installed to run this test");
        assert!(status.success());

        let metadata = VideoAnalyzer::analyze(&input).await.unwrap();
        assert_eq!(metadata.codec, "h264");
        assert!(metadata.filesize_bytes > 0);
    }
}
