use crate::config::{ConversionConfig, RetryConfig};
use crate::db::{Repository, Video};
use crate::discord::DiscordNotifier;
use crate::error::retry::{compute_backoff, is_retryable_message};
use crate::nas::SmbClient;
use crate::progress::{send_progress, ProgressEvent, ProgressSender};
use crate::video::{EncodingOptimizer, VideoAnalyzer, VideoConverter, VideoMetadata};
use crate::worker::downloader::Downloader;
use crate::worker::uploader::Uploader;
use futures::future::BoxFuture;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, info, warn};

/// `movies/foo.mp4` -> `foo.mp4`. Shared by [`ProcessorOrchestrator`]'s
/// `JobStarted` filename and the download stage's local file naming.
fn file_name_of(remote_path: &str) -> String {
    remote_path
        .rsplit('/')
        .next()
        .unwrap_or(remote_path)
        .to_string()
}

/// One stage of the download -> analyze -> convert -> upload pipeline,
/// abstracted behind a trait so [`ProcessorOrchestrator::process_one`] --
/// the actual subject of this ticket -- can be unit-tested against fakes
/// for each stage's success/failure behavior, independent of whether
/// ffmpeg/ffprobe/smbclient (#7/#8/#9) are installed on the machine running
/// the tests.
pub trait DownloadStage: Send + Sync {
    /// `video_id`/`progress_tx` (#20) let implementations emit fine-grained
    /// `TransferProgress` events as the download runs; stage-level
    /// `StageStarted`/`StageCompleted`/`StageFailed` are emitted uniformly
    /// by [`ProcessorOrchestrator`] around this call, not by the stage
    /// itself.
    fn download(
        &self,
        remote_path: String,
        local_path: PathBuf,
        video_id: String,
        progress_tx: ProgressSender,
    ) -> BoxFuture<'static, Result<(), String>>;
}

pub trait AnalyzeStage: Send + Sync {
    fn analyze(&self, local_path: PathBuf) -> BoxFuture<'static, Result<VideoMetadata, String>>;
}

pub trait ConvertStage: Send + Sync {
    /// `video_id`/`progress_tx` (#20) let implementations emit
    /// `EncodingProgress` events as ffmpeg runs (`metadata.duration_secs`
    /// supplies the total against which frame-rate-based ETAs are
    /// estimated).
    fn convert(
        &self,
        input_path: PathBuf,
        output_path: PathBuf,
        metadata: VideoMetadata,
        video_id: String,
        progress_tx: ProgressSender,
    ) -> BoxFuture<'static, Result<u64, String>>;
}

pub trait UploadStage: Send + Sync {
    /// `video_id`/`progress_tx` (#20) let implementations emit
    /// `TransferProgress` events as the upload runs.
    fn upload(
        &self,
        local_path: PathBuf,
        remote_path: String,
        video_id: String,
        progress_tx: ProgressSender,
    ) -> BoxFuture<'static, Result<(), String>>;
}

/// [`DownloadStage`] backed by the real [`SmbClient`]/[`Downloader`] (#8).
pub struct RealDownloadStage(pub Arc<SmbClient>);

impl DownloadStage for RealDownloadStage {
    fn download(
        &self,
        remote_path: String,
        local_path: PathBuf,
        video_id: String,
        progress_tx: ProgressSender,
    ) -> BoxFuture<'static, Result<(), String>> {
        let client = self.0.clone();
        Box::pin(async move {
            Downloader::download_from_nas_with_progress(
                &client,
                &remote_path,
                &local_path,
                &video_id,
                &progress_tx,
            )
            .await
            .map_err(|e| format!("download failed: {e}"))
        })
    }
}

/// [`AnalyzeStage`] backed by the real `ffprobe`-driven [`VideoAnalyzer`] (#6).
pub struct RealAnalyzeStage;

impl AnalyzeStage for RealAnalyzeStage {
    fn analyze(&self, local_path: PathBuf) -> BoxFuture<'static, Result<VideoMetadata, String>> {
        Box::pin(async move {
            VideoAnalyzer::analyze(&local_path)
                .await
                .map_err(|e| format!("analyze failed: {e}"))
        })
    }
}

/// [`ConvertStage`] backed by the real [`VideoConverter`] (#7), with
/// per-file parameters derived by [`EncodingOptimizer`] (#19) rather than
/// applying `conversion_config`'s static preset/crf to every job.
pub struct RealConvertStage {
    pub converter: Arc<VideoConverter>,
    pub conversion_config: ConversionConfig,
}

impl ConvertStage for RealConvertStage {
    fn convert(
        &self,
        input_path: PathBuf,
        output_path: PathBuf,
        metadata: VideoMetadata,
        video_id: String,
        progress_tx: ProgressSender,
    ) -> BoxFuture<'static, Result<u64, String>> {
        let converter = self.converter.clone();
        let conversion_config = self.conversion_config.clone();
        Box::pin(async move {
            // Best-effort core count; a host where this can't be determined
            // is treated as single-core, which just means `threads_per_job`
            // falls back to 1 rather than failing the job.
            let total_cores = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            let params =
                EncodingOptimizer::optimize_params(&metadata, &conversion_config, total_cores);
            let video_codec = if conversion_config.codec == "h265" {
                "libx265"
            } else {
                "libsvtav1"
            };
            converter
                .convert_with_progress(
                    input_path,
                    output_path,
                    video_codec,
                    &params,
                    &video_id,
                    metadata.duration_secs,
                    &progress_tx,
                )
                .await
                .map_err(|e| format!("convert failed: {e}"))
        })
    }
}

/// [`UploadStage`] backed by the real [`SmbClient`]/[`Uploader`] (#9).
pub struct RealUploadStage(pub Arc<SmbClient>);

impl UploadStage for RealUploadStage {
    fn upload(
        &self,
        local_path: PathBuf,
        remote_path: String,
        video_id: String,
        progress_tx: ProgressSender,
    ) -> BoxFuture<'static, Result<(), String>> {
        let client = self.0.clone();
        Box::pin(async move {
            Uploader::upload_to_nas_with_progress(
                &client,
                &local_path,
                &remote_path,
                &video_id,
                &progress_tx,
            )
            .await
            .map_err(|e| format!("upload failed: {e}"))
        })
    }
}

/// Orchestrates the full download -> analyze -> convert -> upload pipeline
/// for one claimed [`Video`], persisting intermediate status/results/
/// failures to the DB at each phase. [`crate::worker::JobQueue`] (#10)
/// decides *when* and *how many* videos are processed concurrently; this
/// type decides what "processing one video" actually means.
///
/// Steps run strictly sequentially -- conversion cannot start before the
/// download completes -- so there is no internal parallelism here; only
/// `JobQueue` parallelizes across distinct videos.
pub struct ProcessorOrchestrator {
    download: Arc<dyn DownloadStage>,
    analyze: Arc<dyn AnalyzeStage>,
    convert: Arc<dyn ConvertStage>,
    upload: Arc<dyn UploadStage>,
    repository: Arc<Repository>,
    /// Local scratch directory; each job gets `work_dir/<video_id>/`.
    work_dir: PathBuf,
    /// `"av1"` or `"h265"` (mirrors `ConversionConfig::codec`); used only to
    /// pick the remote output file's suffix, not the encoder itself (the
    /// convert stage decides that from its own config).
    codec: String,
    /// Retry policy (#17) applied to the download/upload stages -- the ones
    /// that actually talk to the NAS over the network (#8/#9) and can fail
    /// transiently. Analyze/convert failures are typically deterministic
    /// (a corrupt file, a missing binary, a bad ffmpeg parameter) and are
    /// not retried.
    retry_config: RetryConfig,
    /// Progress event sender (#20). Sending is fire-and-forget (see
    /// [`send_progress`]) so a slow/absent display consumer can never stall
    /// the pipeline.
    progress_tx: ProgressSender,
    /// Discord webhook notifier: `None` when Discord notifications are
    /// disabled/unconfigured, in which case [`Self::spawn_discord_notification`]
    /// is a no-op. Notifications are advisory only -- see that method's docs
    /// for why they're fired via `tokio::spawn` rather than awaited inline.
    discord_notifier: Option<Arc<DiscordNotifier>>,
}

impl ProcessorOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        download: Arc<dyn DownloadStage>,
        analyze: Arc<dyn AnalyzeStage>,
        convert: Arc<dyn ConvertStage>,
        upload: Arc<dyn UploadStage>,
        repository: Arc<Repository>,
        work_dir: PathBuf,
        codec: String,
        retry_config: RetryConfig,
        progress_tx: ProgressSender,
        discord_notifier: Option<Arc<DiscordNotifier>>,
    ) -> Self {
        Self {
            download,
            analyze,
            convert,
            upload,
            repository,
            work_dir,
            codec,
            retry_config,
            progress_tx,
            discord_notifier,
        }
    }

    /// Convenience constructor wiring up the real (#8/#6/#7/#9)-backed
    /// stages, as used in production by [`crate::worker::PipelineRunner`].
    #[allow(clippy::too_many_arguments)]
    pub fn from_real(
        smb_client: Arc<SmbClient>,
        converter: Arc<VideoConverter>,
        repository: Arc<Repository>,
        work_dir: PathBuf,
        conversion_config: ConversionConfig,
        retry_config: RetryConfig,
        progress_tx: ProgressSender,
        discord_notifier: Option<Arc<DiscordNotifier>>,
    ) -> Self {
        let codec = conversion_config.codec.clone();
        Self::new(
            Arc::new(RealDownloadStage(smb_client.clone())),
            Arc::new(RealAnalyzeStage),
            Arc::new(RealConvertStage {
                converter,
                conversion_config,
            }),
            Arc::new(RealUploadStage(smb_client)),
            repository,
            work_dir,
            codec,
            retry_config,
            progress_tx,
            discord_notifier,
        )
    }

    /// Run `op` (one call to a [`DownloadStage`]/[`UploadStage`]), retrying
    /// on a retryable failure (per [`is_retryable_message`]) with
    /// exponential backoff up to `self.retry_config.max_attempts`, and
    /// bumping the video's `attempts`/`last_retry_time` (#17) each time a
    /// retry is about to happen.
    ///
    /// The exact original error message is preserved when nothing was ever
    /// retried (`attempt == 1`, e.g. a non-retryable error, or
    /// `max_attempts <= 1`); once at least one retry has actually happened,
    /// a final failure is annotated with how many attempts were made, per
    /// #17's DoD ("statut final failed avec error_message explicite incluant
    /// le nombre de tentatives").
    async fn run_stage_with_retry<T, F, Fut>(
        &self,
        video_id: &str,
        stage_name: &str,
        mut op: F,
    ) -> Result<T, String>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, String>>,
    {
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            match op().await {
                Ok(value) => return Ok(value),
                Err(message) => {
                    let retryable = is_retryable_message(&message);
                    if retryable && attempt < self.retry_config.max_attempts.max(1) {
                        if let Err(db_err) = self.repository.increment_attempts(video_id).await {
                            error!(error = %db_err, video_id, "failed to persist retry attempt count");
                        }
                        let backoff = compute_backoff(attempt, &self.retry_config);
                        warn!(
                            video_id,
                            stage = stage_name,
                            attempt,
                            ?backoff,
                            error = %message,
                            "retrying stage after transient error"
                        );
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    if retryable && attempt > 1 {
                        return Err(format!(
                            "{stage_name} failed after {attempt} attempts: {message}"
                        ));
                    }
                    return Err(message);
                }
            }
        }
    }

    /// Wrap `fut` (one stage's work) with a uniform `StageStarted` before it
    /// and `StageCompleted`/`StageFailed` after it (#20) -- centralizing
    /// stage lifecycle events here means every stage (download/analyze/
    /// convert/upload) reports consistently regardless of how its own
    /// implementation is structured (e.g. `download`/`upload` additionally
    /// go through `run_stage_with_retry`, `analyze`/`convert` don't).
    async fn run_stage_with_events<T, Fut>(
        &self,
        video_id: &str,
        stage_name: &str,
        fut: Fut,
    ) -> Result<T, String>
    where
        Fut: Future<Output = Result<T, String>>,
    {
        send_progress(
            &self.progress_tx,
            ProgressEvent::StageStarted {
                stage_name: stage_name.to_string(),
                video_id: video_id.to_string(),
            },
        );
        let start = Instant::now();
        let result = fut.await;
        match &result {
            Ok(_) => send_progress(
                &self.progress_tx,
                ProgressEvent::StageCompleted {
                    stage_name: stage_name.to_string(),
                    video_id: video_id.to_string(),
                    duration_secs: start.elapsed().as_secs_f64(),
                },
            ),
            Err(message) => send_progress(
                &self.progress_tx,
                ProgressEvent::StageFailed {
                    stage_name: stage_name.to_string(),
                    video_id: video_id.to_string(),
                    error: message.clone(),
                },
            ),
        }
        result
    }

    /// Run the full pipeline for `video`. On any step's failure, persists
    /// `status = 'failed'` + a contextualized `error_message` (e.g.
    /// `"download failed: connection timeout"`) via
    /// [`Repository::fail_video`] -- a single atomic write, so the DB can
    /// never be left with a stale intermediate status and no error message
    /// (see #11's plan). Note: `JobQueue::claim_and_spawn` also calls
    /// `fail_video` when this returns `Err`; that second write is a no-op
    /// (same final state), kept so `JobQueue`'s own contract/tests (#10)
    /// don't have to assume the runner already persisted the failure.
    ///
    /// Also emits `JobStarted`/`JobCompleted`/`JobFailed` (#20) around the
    /// whole run.
    pub async fn process_one(&self, video: Video) -> Result<(), String> {
        send_progress(
            &self.progress_tx,
            ProgressEvent::JobStarted {
                video_id: video.id.clone(),
                filename: file_name_of(&video.file_path),
            },
        );
        let start = Instant::now();

        match self.process_one_inner(&video).await {
            Ok((size_bytes, duration_secs)) => {
                send_progress(
                    &self.progress_tx,
                    ProgressEvent::JobCompleted {
                        video_id: video.id.clone(),
                        total_duration_secs: start.elapsed().as_secs_f64(),
                    },
                );
                self.spawn_discord_notification(&video, size_bytes, duration_secs, true, None);
                Ok(())
            }
            Err(message) => {
                send_progress(
                    &self.progress_tx,
                    ProgressEvent::JobFailed {
                        video_id: video.id.clone(),
                        error: message.clone(),
                    },
                );
                if let Err(db_err) = self.repository.fail_video(&video.id, &message).await {
                    error!(error = %db_err, video_id = %video.id, "failed to persist job failure to DB");
                }
                self.spawn_discord_notification(&video, 0, 0, false, Some(&message));
                Err(message)
            }
        }
    }

    /// Post a "conversion complete"/"conversion failed" Discord notification
    /// (via [`DiscordNotifier::send_conversion_complete`]) in the
    /// background, if a notifier is configured.
    ///
    /// Fire-and-forget by design: the spawned task's `JoinHandle` is
    /// dropped immediately rather than awaited, so a slow or unreachable
    /// Discord webhook can never delay `process_one` returning (and thus
    /// never delays `JobQueue` freeing this job's concurrency-limiting
    /// semaphore permit for the next one). A failed send is logged once and
    /// not retried -- notifications are advisory, not part of the job's
    /// durable state (which is already persisted to the DB by this point).
    fn spawn_discord_notification(
        &self,
        video: &Video,
        file_size_bytes: u64,
        duration_secs: u64,
        success: bool,
        error_message: Option<&str>,
    ) {
        let Some(notifier) = self.discord_notifier.clone() else {
            return;
        };
        let video_id = video.id.clone();
        let error_message = error_message.map(|s| s.to_string());
        tokio::spawn(async move {
            if let Err(e) = notifier
                .send_conversion_complete(
                    &video_id,
                    file_size_bytes,
                    duration_secs,
                    success,
                    error_message.as_deref(),
                )
                .await
            {
                error!(error = %e, video_id = %video_id, "failed to send Discord notification");
            }
        });
    }

    /// Run the full download -> analyze -> convert -> upload pipeline for
    /// one video. Returns the final size on disk (bytes) and the source
    /// duration (whole seconds) on success -- used by [`Self::process_one`]
    /// to populate the Discord "conversion complete" notification -- or an
    /// error message on failure.
    async fn process_one_inner(&self, video: &Video) -> Result<(u64, u64), String> {
        let sanitized_id = video
            .id
            .trim_start_matches('/')
            .chars()
            .map(|c| if c == '/' || c == '\\' { '_' } else { c })
            .collect::<String>();
        let job_dir = self.work_dir.join(&sanitized_id);
        tokio::fs::create_dir_all(&job_dir)
            .await
            .map_err(|e| format!("failed to create work dir: {e}"))?;

        let source_file_name = file_name_of(&video.file_path);
        let source_path = job_dir.join(&source_file_name);

        // 1. Download. Retried (#17) on a transient failure (e.g. the NAS
        // connection dropping mid-transfer) since a fresh attempt is likely
        // to succeed; a permanent failure (auth, file not found) is not.
        self.run_stage_with_events(
            &video.id,
            "download",
            self.run_stage_with_retry(&video.id, "download", || {
                self.download.download(
                    video.file_path.clone(),
                    source_path.clone(),
                    video.id.clone(),
                    self.progress_tx.clone(),
                )
            }),
        )
        .await?;

        // 2. Analyze + persist source metadata. Not retried: an
        // ffprobe/parse failure here is almost always deterministic (a
        // genuinely corrupt or unsupported file), so retrying would just
        // waste time reproducing the same failure.
        let metadata = self
            .run_stage_with_events(
                &video.id,
                "analyze",
                self.analyze.analyze(source_path.clone()),
            )
            .await?;
        self.repository
            .update_video_metadata(
                &video.id,
                &metadata.codec,
                metadata.bitrate_kbps as i32,
                metadata.filesize_bytes as i64,
            )
            .await
            .map_err(|e| format!("failed to persist metadata: {e}"))?;

        if !VideoAnalyzer::should_convert(&metadata) {
            info!(video_id = %video.id, "video already meets target codec/size, skipping conversion");
            self.repository
                .update_video_result(&video.id, metadata.filesize_bytes as i64, "done")
                .await
                .map_err(|e| format!("failed to persist result: {e}"))?;
            let _ = tokio::fs::remove_dir_all(&job_dir).await;
            return Ok((
                metadata.filesize_bytes,
                metadata.duration_secs.round() as u64,
            ));
        }

        self.repository
            .update_video_status(&video.id, "converting")
            .await
            .map_err(|e| format!("failed to update status: {e}"))?;

        // 3. Convert. ffmpeg is asked to remux into Matroska regardless of
        // target codec, since both AV1 and H.265 streams fit fine in `.mkv`
        // and it keeps the output naming uniform.
        //
        // `duration_secs` is captured before `metadata` is moved into
        // `self.convert.convert(...)` below, so it's still available after
        // the upload for the Discord notification fired by `process_one`.
        let duration_secs = metadata.duration_secs.round() as u64;
        let converted_path = job_dir.join("converted.mkv");
        let converted_size = self
            .run_stage_with_events(
                &video.id,
                "convert",
                self.convert.convert(
                    source_path.clone(),
                    converted_path.clone(),
                    metadata,
                    video.id.clone(),
                    self.progress_tx.clone(),
                ),
            )
            .await?;

        self.repository
            .update_video_status(&video.id, "uploading")
            .await
            .map_err(|e| format!("failed to update status: {e}"))?;

        // 4. Upload next to the original with a codec suffix, rather than
        // overwriting it -- see #9's plan: replacing the original in place
        // is a product decision left open, so we don't destroy data by
        // guessing.
        //
        // On failure here, `job_dir` (including the already-downloaded
        // source and the already-converted output) is deliberately left on
        // disk rather than cleaned up, so a retry (#17) doesn't have to
        // redo the download+convert work.
        let remote_output_path = remote_converted_path(&video.file_path, &self.codec);
        self.run_stage_with_events(
            &video.id,
            "upload",
            self.run_stage_with_retry(&video.id, "upload", || {
                self.upload.upload(
                    converted_path.clone(),
                    remote_output_path.clone(),
                    video.id.clone(),
                    self.progress_tx.clone(),
                )
            }),
        )
        .await?;

        self.repository
            .update_video_result(&video.id, converted_size as i64, "done")
            .await
            .map_err(|e| format!("failed to persist result: {e}"))?;

        let _ = tokio::fs::remove_dir_all(&job_dir).await;
        Ok((converted_size, duration_secs))
    }
}

/// `movies/foo.mp4` + `"av1"` -> `movies/foo_av1.mkv`.
pub fn remote_converted_path(original_remote_path: &str, codec: &str) -> String {
    let (dir, file) = match original_remote_path.rsplit_once('/') {
        Some((d, f)) => (Some(d), f),
        None => (None, original_remote_path),
    };
    let stem = file.rsplit_once('.').map(|(s, _)| s).unwrap_or(file);
    let new_file = format!("{stem}_{codec}.mkv");
    match dir {
        Some(d) => format!("{d}/{new_file}"),
        None => new_file,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::connection::DbConnection;
    use chrono::Utc;

    fn make_video(id: &str, file_path: &str) -> Video {
        Video {
            id: id.to_string(),
            file_path: file_path.to_string(),
            status: "pending".to_string(),
            original_codec: None,
            original_bitrate_kbps: None,
            original_size_bytes: None,
            converted_size_bytes: None,
            instance_id: None,
            error_message: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            claimed_at: None,
            attempts: 0,
            last_retry_time: None,
        }
    }

    fn h264_metadata() -> VideoMetadata {
        VideoMetadata {
            codec: "h264".to_string(),
            bitrate_kbps: 3000,
            resolution: "1080p".to_string(),
            filesize_bytes: 1_000_000_000,
            duration_secs: 3600.0,
        }
    }

    fn already_av1_metadata() -> VideoMetadata {
        VideoMetadata {
            codec: "av1".to_string(),
            bitrate_kbps: 2000,
            resolution: "1080p".to_string(),
            filesize_bytes: 500_000_000,
            duration_secs: 3600.0,
        }
    }

    async fn repo_with_video(video: &Video) -> (tempfile::TempDir, Arc<Repository>) {
        let dir = tempfile::tempdir().unwrap();
        let conn = DbConnection::new(dir.path().join("test.db")).await.unwrap();
        let repo = Repository::new(conn.pool().clone());
        repo.insert_video(video).await.unwrap();
        (dir, Arc::new(repo))
    }

    /// A [`ProgressSender`] with no live [`crate::progress::ProgressDisplay`]
    /// consuming it -- fine for tests, since [`send_progress`] silently
    /// drops events once the receiver is gone rather than erroring/panicking.
    fn test_progress_tx() -> ProgressSender {
        crate::progress::channel().0
    }

    struct FakeDownload {
        result: Result<(), String>,
    }
    impl DownloadStage for FakeDownload {
        fn download(
            &self,
            _remote: String,
            _local: PathBuf,
            _video_id: String,
            _progress_tx: ProgressSender,
        ) -> BoxFuture<'static, Result<(), String>> {
            let result = self.result.clone();
            Box::pin(async move { result })
        }
    }

    struct FakeAnalyze {
        result: Result<VideoMetadata, String>,
    }
    impl AnalyzeStage for FakeAnalyze {
        fn analyze(&self, _local: PathBuf) -> BoxFuture<'static, Result<VideoMetadata, String>> {
            let result = self.result.clone();
            Box::pin(async move { result })
        }
    }

    struct FakeConvert {
        result: Result<u64, String>,
    }
    impl ConvertStage for FakeConvert {
        fn convert(
            &self,
            _input: PathBuf,
            _output: PathBuf,
            _metadata: VideoMetadata,
            _video_id: String,
            _progress_tx: ProgressSender,
        ) -> BoxFuture<'static, Result<u64, String>> {
            let result = self.result.clone();
            Box::pin(async move { result })
        }
    }

    struct FakeUpload {
        result: Result<(), String>,
    }
    impl UploadStage for FakeUpload {
        fn upload(
            &self,
            _local: PathBuf,
            _remote: String,
            _video_id: String,
            _progress_tx: ProgressSender,
        ) -> BoxFuture<'static, Result<(), String>> {
            let result = self.result.clone();
            Box::pin(async move { result })
        }
    }

    /// Retries disabled (`max_attempts: 1`): these fixed-result fakes always
    /// return the same `Result` on every call, so retrying would either be a
    /// no-op (permanent errors) or loop forever reproducing the same
    /// "failure" for an error message that merely happens to contain a
    /// retryable-looking substring (e.g. "connection timeout" in the
    /// existing failure tests below, predating #17). Retry behavior itself
    /// is exercised separately by the `test_process_one_retries_*` tests,
    /// which use stateful fakes.
    fn no_retry_config() -> RetryConfig {
        RetryConfig {
            max_attempts: 1,
            base_delay_secs: 0,
            max_delay_secs: 0,
        }
    }

    fn orchestrator(
        repo: Arc<Repository>,
        work_dir: PathBuf,
        download: Result<(), String>,
        analyze: Result<VideoMetadata, String>,
        convert: Result<u64, String>,
        upload: Result<(), String>,
    ) -> ProcessorOrchestrator {
        ProcessorOrchestrator::new(
            Arc::new(FakeDownload { result: download }),
            Arc::new(FakeAnalyze { result: analyze }),
            Arc::new(FakeConvert { result: convert }),
            Arc::new(FakeUpload { result: upload }),
            repo,
            work_dir,
            "av1".to_string(),
            no_retry_config(),
            test_progress_tx(),
            None,
        )
    }

    /// Same as [`orchestrator`], but with a Discord notifier wired in
    /// (pointed at a caller-supplied webhook URL, typically a `mockito`
    /// server) -- used by the notification-specific tests below, kept
    /// separate so the existing tests above don't have to care about
    /// Discord at all.
    #[allow(clippy::too_many_arguments)]
    fn orchestrator_with_discord(
        repo: Arc<Repository>,
        work_dir: PathBuf,
        download: Result<(), String>,
        analyze: Result<VideoMetadata, String>,
        convert: Result<u64, String>,
        upload: Result<(), String>,
        webhook_url: String,
    ) -> ProcessorOrchestrator {
        ProcessorOrchestrator::new(
            Arc::new(FakeDownload { result: download }),
            Arc::new(FakeAnalyze { result: analyze }),
            Arc::new(FakeConvert { result: convert }),
            Arc::new(FakeUpload { result: upload }),
            repo,
            work_dir,
            "av1".to_string(),
            no_retry_config(),
            test_progress_tx(),
            Some(Arc::new(DiscordNotifier::new(webhook_url))),
        )
    }

    #[tokio::test]
    async fn test_process_one_full_success_marks_done_with_converted_size() {
        let video = make_video("v1", "/videos/v1.mp4");
        let (dir, repo) = repo_with_video(&video).await;
        let work_dir = dir.path().join("work");

        let orch = orchestrator(
            repo.clone(),
            work_dir,
            Ok(()),
            Ok(h264_metadata()),
            Ok(42_000),
            Ok(()),
        );

        let result = orch.process_one(video).await;
        assert!(result.is_ok(), "{result:?}");

        let updated = repo.get_video("v1").await.unwrap().unwrap();
        assert_eq!(updated.status, "done");
        assert_eq!(updated.converted_size_bytes, Some(42_000));
        assert_eq!(updated.original_codec.as_deref(), Some("h264"));
    }

    #[tokio::test]
    async fn test_process_one_short_circuits_when_should_convert_is_false() {
        let video = make_video("v1", "/videos/v1.mp4");
        let (dir, repo) = repo_with_video(&video).await;
        let work_dir = dir.path().join("work");

        // Convert/upload would fail if ever called, proving the short
        // circuit actually skips them.
        let orch = orchestrator(
            repo.clone(),
            work_dir,
            Ok(()),
            Ok(already_av1_metadata()),
            Err("convert must not be called".to_string()),
            Err("upload must not be called".to_string()),
        );

        let result = orch.process_one(video).await;
        assert!(result.is_ok(), "{result:?}");

        let updated = repo.get_video("v1").await.unwrap().unwrap();
        assert_eq!(updated.status, "done");
        assert_eq!(updated.converted_size_bytes, Some(500_000_000));
    }

    #[tokio::test]
    async fn test_process_one_download_failure_marks_failed_with_context() {
        let video = make_video("v1", "/videos/v1.mp4");
        let (dir, repo) = repo_with_video(&video).await;
        let work_dir = dir.path().join("work");

        let orch = orchestrator(
            repo.clone(),
            work_dir,
            Err("download failed: connection timeout".to_string()),
            Ok(h264_metadata()),
            Ok(1),
            Ok(()),
        );

        let result = orch.process_one(video).await;
        assert!(result.is_err());

        let updated = repo.get_video("v1").await.unwrap().unwrap();
        assert_eq!(updated.status, "failed");
        assert_eq!(
            updated.error_message.as_deref(),
            Some("download failed: connection timeout")
        );
    }

    #[tokio::test]
    async fn test_process_one_convert_failure_marks_failed_and_keeps_status_error_consistent() {
        let video = make_video("v1", "/videos/v1.mp4");
        let (dir, repo) = repo_with_video(&video).await;
        let work_dir = dir.path().join("work");

        let orch = orchestrator(
            repo.clone(),
            work_dir,
            Ok(()),
            Ok(h264_metadata()),
            Err("convert failed: ffmpeg exited with code 1".to_string()),
            Ok(()),
        );

        let result = orch.process_one(video).await;
        assert!(result.is_err());

        let updated = repo.get_video("v1").await.unwrap().unwrap();
        // Must never be left at the intermediate `converting` status with a
        // populated error_message -- status and error_message are written
        // together via `fail_video`.
        assert_eq!(updated.status, "failed");
        assert_eq!(
            updated.error_message.as_deref(),
            Some("convert failed: ffmpeg exited with code 1")
        );
    }

    #[tokio::test]
    async fn test_process_one_upload_failure_marks_failed() {
        let video = make_video("v1", "/videos/v1.mp4");
        let (dir, repo) = repo_with_video(&video).await;
        let work_dir = dir.path().join("work");

        let orch = orchestrator(
            repo.clone(),
            work_dir,
            Ok(()),
            Ok(h264_metadata()),
            Ok(42_000),
            Err("upload failed: NAS unreachable".to_string()),
        );

        let result = orch.process_one(video).await;
        assert!(result.is_err());

        let updated = repo.get_video("v1").await.unwrap().unwrap();
        assert_eq!(updated.status, "failed");
        assert_eq!(
            updated.error_message.as_deref(),
            Some("upload failed: NAS unreachable")
        );
    }

    #[tokio::test]
    async fn test_process_one_analyze_failure_marks_failed() {
        let video = make_video("v1", "/videos/v1.mp4");
        let (dir, repo) = repo_with_video(&video).await;
        let work_dir = dir.path().join("work");

        let orch = orchestrator(
            repo.clone(),
            work_dir,
            Ok(()),
            Err("analyze failed: ffprobe not found".to_string()),
            Ok(1),
            Ok(()),
        );

        let result = orch.process_one(video).await;
        assert!(result.is_err());

        let updated = repo.get_video("v1").await.unwrap().unwrap();
        assert_eq!(updated.status, "failed");
        assert_eq!(
            updated.error_message.as_deref(),
            Some("analyze failed: ffprobe not found")
        );
    }

    #[test]
    fn test_remote_converted_path_nested() {
        assert_eq!(
            remote_converted_path("movies/2024/foo.mp4", "av1"),
            "movies/2024/foo_av1.mkv"
        );
    }

    #[test]
    fn test_remote_converted_path_root_level() {
        assert_eq!(remote_converted_path("foo.mp4", "av1"), "foo_av1.mkv");
    }

    // --- #17: retry policy around the download/upload stages ---

    /// Fails with a retryable-looking message the first `fail_count` calls,
    /// then succeeds. Used to prove `run_stage_with_retry` actually retries
    /// rather than just classifying errors.
    struct FlakyDownload {
        remaining_failures: std::sync::atomic::AtomicU32,
        calls: std::sync::atomic::AtomicU32,
    }

    impl FlakyDownload {
        fn new(fail_count: u32) -> Self {
            Self {
                remaining_failures: std::sync::atomic::AtomicU32::new(fail_count),
                calls: std::sync::atomic::AtomicU32::new(0),
            }
        }
    }

    impl DownloadStage for FlakyDownload {
        fn download(
            &self,
            _remote: String,
            _local: PathBuf,
            _video_id: String,
            _progress_tx: ProgressSender,
        ) -> BoxFuture<'static, Result<(), String>> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let remaining = self.remaining_failures.fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |n| if n > 0 { Some(n - 1) } else { Some(0) },
            );
            let should_fail = remaining.map(|n| n > 0).unwrap_or(false);
            Box::pin(async move {
                if should_fail {
                    Err("download failed: connection timeout".to_string())
                } else {
                    Ok(())
                }
            })
        }
    }

    fn retrying_orchestrator(
        repo: Arc<Repository>,
        work_dir: PathBuf,
        download: Arc<dyn DownloadStage>,
        upload: Result<(), String>,
        max_attempts: u32,
    ) -> ProcessorOrchestrator {
        ProcessorOrchestrator::new(
            download,
            Arc::new(FakeAnalyze {
                result: Ok(h264_metadata()),
            }),
            Arc::new(FakeConvert { result: Ok(1) }),
            Arc::new(FakeUpload { result: upload }),
            repo,
            work_dir,
            "av1".to_string(),
            RetryConfig {
                max_attempts,
                base_delay_secs: 0,
                max_delay_secs: 0,
            },
            test_progress_tx(),
            None,
        )
    }

    #[tokio::test]
    async fn test_process_one_retries_transient_download_failure_then_succeeds() {
        let video = make_video("v1", "/videos/v1.mp4");
        let (dir, repo) = repo_with_video(&video).await;
        let work_dir = dir.path().join("work");

        let download = Arc::new(FlakyDownload::new(2)); // fails twice, then succeeds
        let orch = retrying_orchestrator(repo.clone(), work_dir, download.clone(), Ok(()), 5);

        tokio::time::pause();
        let result = orch.process_one(video).await;
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(
            download.calls.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "expected exactly 2 failed attempts + 1 successful attempt"
        );

        let updated = repo.get_video("v1").await.unwrap().unwrap();
        assert_eq!(updated.status, "done");
        assert_eq!(
            updated.attempts, 2,
            "attempts should be incremented once per retry, not per call"
        );
        assert!(updated.last_retry_time.is_some());
    }

    #[tokio::test]
    async fn test_process_one_exhausts_retries_and_reports_attempt_count() {
        let video = make_video("v1", "/videos/v1.mp4");
        let (dir, repo) = repo_with_video(&video).await;
        let work_dir = dir.path().join("work");

        // Always fails with a retryable-looking message.
        let download = Arc::new(FlakyDownload::new(u32::MAX));
        let orch = retrying_orchestrator(repo.clone(), work_dir, download.clone(), Ok(()), 3);

        tokio::time::pause();
        let result = orch.process_one(video).await;
        assert!(result.is_err());
        assert_eq!(
            download.calls.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "must stop at max_attempts, not retry forever"
        );

        let updated = repo.get_video("v1").await.unwrap().unwrap();
        assert_eq!(updated.status, "failed");
        let message = updated.error_message.unwrap();
        assert!(
            message.contains("after 3 attempts"),
            "error message should report the attempt count: {message}"
        );
        assert_eq!(
            updated.attempts, 2,
            "one increment per retry (2 retries before giving up)"
        );
    }

    #[tokio::test]
    async fn test_process_one_does_not_retry_non_retryable_download_failure() {
        let video = make_video("v1", "/videos/v1.mp4");
        let (dir, repo) = repo_with_video(&video).await;
        let work_dir = dir.path().join("work");

        let orch = orchestrator(
            repo.clone(),
            work_dir,
            Err("authentication failed for user 'bob'".to_string()),
            Ok(h264_metadata()),
            Ok(1),
            Ok(()),
        );

        let result = orch.process_one(video).await;
        assert!(result.is_err());

        let updated = repo.get_video("v1").await.unwrap().unwrap();
        assert_eq!(updated.status, "failed");
        assert_eq!(
            updated.error_message.as_deref(),
            Some("authentication failed for user 'bob'"),
            "a non-retryable error's message must be preserved verbatim, with no attempt-count suffix"
        );
        assert_eq!(
            updated.attempts, 0,
            "a non-retryable error must never increment attempts"
        );
    }

    // --- Discord notifications ---

    /// The background `tokio::spawn`ed notification task races with the
    /// test's own assertions; `process_one` deliberately doesn't await it
    /// (see `spawn_discord_notification`'s docs), so tests give it a short
    /// window to land before asserting on the mock.
    async fn wait_for_background_notification() {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    #[tokio::test]
    async fn test_process_one_success_fires_discord_notification_without_blocking_result() {
        let video = make_video("v1", "/videos/v1.mp4");
        let (dir, repo) = repo_with_video(&video).await;
        let work_dir = dir.path().join("work");

        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/webhook")
            .with_status(204)
            .create_async()
            .await;

        let orch = orchestrator_with_discord(
            repo.clone(),
            work_dir,
            Ok(()),
            Ok(h264_metadata()),
            Ok(42_000),
            Ok(()),
            format!("{}/webhook", server.url()),
        );

        let result = orch.process_one(video).await;
        assert!(result.is_ok(), "{result:?}");

        wait_for_background_notification().await;
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_process_one_failure_fires_discord_notification_with_error_message() {
        let video = make_video("v1", "/videos/v1.mp4");
        let (dir, repo) = repo_with_video(&video).await;
        let work_dir = dir.path().join("work");

        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/webhook")
            .with_status(204)
            .match_body(mockito::Matcher::Regex(
                "convert failed: ffmpeg exited with code 1".to_string(),
            ))
            .create_async()
            .await;

        let orch = orchestrator_with_discord(
            repo.clone(),
            work_dir,
            Ok(()),
            Ok(h264_metadata()),
            Err("convert failed: ffmpeg exited with code 1".to_string()),
            Ok(()),
            format!("{}/webhook", server.url()),
        );

        let result = orch.process_one(video).await;
        assert!(result.is_err());

        wait_for_background_notification().await;
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_process_one_no_discord_notifier_configured_does_not_panic_or_block() {
        // `orchestrator()` (used by every other test in this file) passes
        // `None` for the notifier -- this test just makes that omission
        // explicit and confirms `process_one` still completes normally.
        let video = make_video("v1", "/videos/v1.mp4");
        let (dir, repo) = repo_with_video(&video).await;
        let work_dir = dir.path().join("work");

        let orch = orchestrator(
            repo.clone(),
            work_dir,
            Ok(()),
            Ok(h264_metadata()),
            Ok(42_000),
            Ok(()),
        );

        let result = orch.process_one(video).await;
        assert!(result.is_ok(), "{result:?}");
    }
}
