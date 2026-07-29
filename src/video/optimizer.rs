use super::models::VideoMetadata;
use crate::config::ConversionConfig;

/// Concrete ffmpeg parameters computed for one specific source file, as
/// opposed to `ConversionConfig`'s static, operator-configured
/// preset/crf/max_parallel_jobs which now act as a baseline/floor rather
/// than the literal values passed to ffmpeg (see #19's plan).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodingParams {
    pub preset: String,
    pub crf: u8,
    /// Threads/logical-processors this single encode should use, derived
    /// from the host's core count and `max_parallel_jobs` so concurrent
    /// jobs don't contend for the same cores (see
    /// `EncodingOptimizer::compute_threads_per_job`).
    pub threads_per_job: usize,
    /// Extra ffmpeg CLI args appended after `-crf`, e.g.
    /// `["-svtav1-params", "lp=4"]` for AV1 thread pinning.
    pub extra_args: Vec<String>,
}

/// Derives per-file encoding parameters from a video's measured metadata
/// (#6's `VideoAnalyzer`) instead of applying the same static
/// preset/crf/thread count to every job regardless of content.
pub struct EncodingOptimizer;

impl EncodingOptimizer {
    /// Quality band this optimizer will ever produce, regardless of what
    /// `config.crf` says -- keeps content-aware adjustment inside a sane
    /// range instead of drifting arbitrarily far from the operator's
    /// configured baseline. Mirrors the ticket's "CRF: 25-35" requirement.
    const MIN_CRF: i32 = 25;
    const MAX_CRF: i32 = 35;

    /// File size above which a source is considered "large" for preset
    /// selection -- mirrors the 2GB threshold `VideoAnalyzer::should_convert`
    /// already uses to flag archival-sized files.
    const LARGE_FILE_BYTES: u64 = 2_000_000_000;

    /// Duration above which a source is considered "long" for preset
    /// selection (3 hours), independent of file size (a long but
    /// low-bitrate source can still take a very long time to encode at a
    /// slow preset).
    const LONG_DURATION_SECS: f64 = 3.0 * 3600.0;

    /// Compute the ffmpeg parameters to use for encoding `metadata`, given
    /// the operator's static `config` (used as a baseline/floor) and the
    /// number of CPU cores available on this host.
    pub fn optimize_params(
        metadata: &VideoMetadata,
        config: &ConversionConfig,
        total_cores: usize,
    ) -> EncodingParams {
        let crf = Self::compute_crf(metadata, config);
        let preset = Self::compute_preset(metadata, config);
        let threads_per_job = Self::compute_threads_per_job(total_cores, config.max_parallel_jobs);

        let mut extra_args = Vec::new();
        if config.codec == "av1" {
            // SVT-AV1's own thread-pool knob (`lp` = logical processors);
            // without this, every concurrent ffmpeg job defaults to using
            // *all* cores, so `max_parallel_jobs > 1` just makes jobs
            // contend with each other instead of parallelizing usefully.
            extra_args.push("-svtav1-params".to_string());
            extra_args.push(format!("lp={threads_per_job}"));
        }

        EncodingParams {
            preset,
            crf,
            threads_per_job,
            extra_args,
        }
    }

    /// Nudge `config.crf` within `[MIN_CRF, MAX_CRF]` based on how much
    /// bitrate the source already carries relative to what's typical for
    /// its resolution: a high-bitrate source can absorb a slightly higher
    /// (more compressive) CRF without a visible quality hit, while an
    /// already-lean source should be encoded closer to the
    /// quality-preserving end so we don't compound its existing loss.
    fn compute_crf(metadata: &VideoMetadata, config: &ConversionConfig) -> u8 {
        let mut crf = config.crf as i32;

        let high_bitrate_threshold_kbps: u32 = match metadata.resolution.as_str() {
            "4K" => 15_000,
            "1440p" => 8_000,
            _ => 5_000, // 1080p and below
        };

        if metadata.bitrate_kbps > high_bitrate_threshold_kbps {
            crf += 2;
        } else if metadata.bitrate_kbps > 0
            && metadata.bitrate_kbps < high_bitrate_threshold_kbps / 2
        {
            crf -= 2;
        }

        crf.clamp(Self::MIN_CRF, Self::MAX_CRF) as u8
    }

    /// Cap how slow (and therefore how long-running) an encode can be for
    /// large/long sources, regardless of the operator's configured default
    /// -- a multi-hour 4K file encoded at `slower`/`veryslow` on modest
    /// NAS-adjacent hardware can run for the better part of a day (see
    /// #19's plan edge case). Never picks a *slower* preset than what was
    /// configured, only ever caps toward faster.
    fn compute_preset(metadata: &VideoMetadata, config: &ConversionConfig) -> String {
        const PRESET_ORDER: [&str; 9] = [
            "veryslow",
            "slower",
            "slow",
            "medium",
            "fast",
            "faster",
            "veryfast",
            "superfast",
            "ultrafast",
        ];
        const MEDIUM_RANK: usize = 3;

        let configured_rank = PRESET_ORDER
            .iter()
            .position(|p| *p == config.preset)
            // For numeric presets (e.g., AV1 0-13) or unknown strings,
            // assume roughly as slow as "slow" (rank 2) rather than refusing to cap.
            .unwrap_or(2);

        let is_large_or_long = metadata.filesize_bytes > Self::LARGE_FILE_BYTES
            || metadata.duration_secs > Self::LONG_DURATION_SECS;

        let rank = if is_large_or_long {
            configured_rank.max(MEDIUM_RANK)
        } else {
            configured_rank
        };

        PRESET_ORDER[rank].to_string()
    }

    /// `total_cores / max_parallel_jobs`, rounded down, minimum 1 -- so
    /// `max_parallel_jobs` concurrent encodes divide the host's cores
    /// instead of each trying to claim all of them.
    fn compute_threads_per_job(total_cores: usize, max_parallel_jobs: usize) -> usize {
        let max_parallel_jobs = max_parallel_jobs.max(1);
        (total_cores / max_parallel_jobs).max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(codec: &str, preset: &str, crf: u8, max_parallel_jobs: usize) -> ConversionConfig {
        ConversionConfig {
            codec: codec.to_string(),
            preset: preset.to_string(),
            crf,
            max_parallel_jobs,
        }
    }

    fn metadata(
        resolution: &str,
        bitrate_kbps: u32,
        filesize_bytes: u64,
        duration_secs: f64,
    ) -> VideoMetadata {
        VideoMetadata {
            codec: "h264".to_string(),
            bitrate_kbps,
            resolution: resolution.to_string(),
            filesize_bytes,
            duration_secs,
        }
    }

    #[test]
    fn test_crf_unchanged_for_typical_1080p_bitrate() {
        let cfg = config("av1", "slow", 32, 3);
        let meta = metadata("1080p", 3000, 1_000_000_000, 3600.0);
        let params = EncodingOptimizer::optimize_params(&meta, &cfg, 12);
        assert!((25..=35).contains(&(params.crf as i32)));
        assert_eq!(params.crf, 32);
    }

    #[test]
    fn test_crf_increases_for_high_bitrate_1080p_source() {
        let cfg = config("av1", "slow", 30, 3);
        let meta = metadata("1080p", 8000, 1_000_000_000, 3600.0);
        let params = EncodingOptimizer::optimize_params(&meta, &cfg, 12);
        assert_eq!(params.crf, 32);
    }

    #[test]
    fn test_crf_decreases_for_low_bitrate_source() {
        let cfg = config("av1", "slow", 30, 3);
        let meta = metadata("1080p", 1000, 500_000_000, 3600.0);
        let params = EncodingOptimizer::optimize_params(&meta, &cfg, 12);
        assert_eq!(params.crf, 28);
    }

    #[test]
    fn test_crf_threshold_is_resolution_aware_for_4k() {
        // 8000 kbps is "high" for 1080p but unremarkable for 4K.
        let cfg = config("av1", "slow", 30, 3);
        let meta = metadata("4K", 8000, 4_000_000_000, 3600.0);
        let params = EncodingOptimizer::optimize_params(&meta, &cfg, 12);
        assert_eq!(params.crf, 30);
    }

    #[test]
    fn test_crf_never_leaves_configured_band() {
        let cfg = config("av1", "slow", 34, 3);
        let meta = metadata("1080p", 999_999, 1_000_000_000, 3600.0);
        let params = EncodingOptimizer::optimize_params(&meta, &cfg, 12);
        assert!(params.crf <= 35);
    }

    #[test]
    fn test_preset_capped_to_medium_for_large_file() {
        let cfg = config("av1", "veryslow", 32, 3);
        let meta = metadata("1080p", 3000, 3_000_000_000, 3600.0); // > 2GB
        let params = EncodingOptimizer::optimize_params(&meta, &cfg, 12);
        assert_eq!(params.preset, "medium");
    }

    #[test]
    fn test_preset_capped_to_medium_for_long_duration() {
        let cfg = config("av1", "slower", 32, 3);
        let meta = metadata("1080p", 3000, 500_000_000, 4.0 * 3600.0); // 4h, small file
        let params = EncodingOptimizer::optimize_params(&meta, &cfg, 12);
        assert_eq!(params.preset, "medium");
    }

    #[test]
    fn test_preset_unchanged_for_small_short_file() {
        let cfg = config("av1", "slow", 32, 3);
        let meta = metadata("1080p", 3000, 500_000_000, 1800.0);
        let params = EncodingOptimizer::optimize_params(&meta, &cfg, 12);
        assert_eq!(params.preset, "slow");
    }

    #[test]
    fn test_preset_never_slowed_down_from_already_fast_config() {
        let cfg = config("av1", "ultrafast", 32, 3);
        let meta = metadata("1080p", 3000, 5_000_000_000, 7200.0);
        let params = EncodingOptimizer::optimize_params(&meta, &cfg, 12);
        assert_eq!(params.preset, "ultrafast");
    }

    #[test]
    fn test_threads_per_job_divides_cores_by_parallel_jobs() {
        let cfg = config("av1", "slow", 32, 3);
        let meta = metadata("1080p", 3000, 1_000_000_000, 3600.0);
        let params = EncodingOptimizer::optimize_params(&meta, &cfg, 12);
        assert_eq!(params.threads_per_job, 4);
    }

    #[test]
    fn test_threads_per_job_minimum_one() {
        let cfg = config("av1", "slow", 32, 8);
        let meta = metadata("1080p", 3000, 1_000_000_000, 3600.0);
        let params = EncodingOptimizer::optimize_params(&meta, &cfg, 4);
        assert_eq!(params.threads_per_job, 1);
    }

    #[test]
    fn test_av1_codec_gets_svtav1_lp_extra_arg() {
        let cfg = config("av1", "slow", 32, 2);
        let meta = metadata("1080p", 3000, 1_000_000_000, 3600.0);
        let params = EncodingOptimizer::optimize_params(&meta, &cfg, 8);
        assert_eq!(
            params.extra_args,
            vec!["-svtav1-params".to_string(), "lp=4".to_string()]
        );
    }

    #[test]
    fn test_h265_codec_has_no_svtav1_extra_arg() {
        let cfg = config("h265", "slow", 32, 2);
        let meta = metadata("1080p", 3000, 1_000_000_000, 3600.0);
        let params = EncodingOptimizer::optimize_params(&meta, &cfg, 8);
        assert!(params.extra_args.is_empty());
    }
}
