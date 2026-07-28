//! End-to-end tests for the local worker pipeline: `JobQueue` (#10) claiming
//! and scheduling work, `ProcessorOrchestrator` (#11) running it, and
//! `Repository::reclaim_stale_jobs` (#17) recovering from a crashed worker
//! -- exercised together against a real (tempfile-backed) SQLite DB, per
//! #16's plan.
//!
//! No real NAS/SMB or ffmpeg/ffprobe is required for the default (non
//! `--ignored`) tests: download/upload/analyze/convert are all fakes here,
//! the same "trait + fake" pattern `worker::processor`'s own unit tests use,
//! just driven through the *real* `JobQueue` rather than calling
//! `ProcessorOrchestrator` directly. The one test that needs real ffmpeg is
//! marked `#[ignore]` (see README's "Running the ignored tests" section).

use chrono::Utc;
use futures::future::BoxFuture;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use trein_video::config::RetryConfig;
use trein_video::db::{DbConnection, Repository, Video};
use trein_video::progress::ProgressSender;
use trein_video::video::VideoMetadata;
use trein_video::worker::job_queue::JobRunner;
use trein_video::worker::processor::{
    AnalyzeStage, ConvertStage, DownloadStage, ProcessorOrchestrator, UploadStage,
};
use trein_video::worker::JobQueue;

/// A [`ProgressSender`] with no live consumer -- fine here since sending is
/// fire-and-forget (see `trein_video::progress::send_progress`) and these
/// integration tests aren't exercising the progress display itself (that's
/// covered by `src/progress`'s own unit tests).
fn test_progress_tx() -> ProgressSender {
    trein_video::progress::channel().0
}

fn pending_video(id: &str) -> Video {
    let now = Utc::now();
    Video {
        id: id.to_string(),
        file_path: format!("/videos/{id}.mp4"),
        status: "pending".to_string(),
        original_codec: None,
        original_bitrate_kbps: None,
        original_size_bytes: None,
        converted_size_bytes: None,
        instance_id: None,
        error_message: None,
        created_at: now,
        updated_at: now,
        claimed_at: None,
        attempts: 0,
        last_retry_time: None,
    }
}

async fn repo_with_pending(count: usize) -> (tempfile::TempDir, Arc<Repository>) {
    let dir = tempfile::tempdir().unwrap();
    let conn = DbConnection::new(dir.path().join("test.db")).await.unwrap();
    let repo = Repository::new(conn.pool().clone());
    for i in 0..count {
        repo.insert_video(&pending_video(&format!("v{i}")))
            .await
            .unwrap();
    }
    (dir, Arc::new(repo))
}

// --- Fake pipeline stages (no ffmpeg/ffprobe/smbclient needed) ---

struct OkDownload;
impl DownloadStage for OkDownload {
    fn download(
        &self,
        _remote: String,
        _local: PathBuf,
        _video_id: String,
        _progress_tx: ProgressSender,
    ) -> BoxFuture<'static, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}

struct OkUpload;
impl UploadStage for OkUpload {
    fn upload(
        &self,
        _local: PathBuf,
        _remote: String,
        _video_id: String,
        _progress_tx: ProgressSender,
    ) -> BoxFuture<'static, Result<(), String>> {
        Box::pin(async { Ok(()) })
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

struct FixedAnalyze(VideoMetadata);
impl AnalyzeStage for FixedAnalyze {
    fn analyze(&self, _local: PathBuf) -> BoxFuture<'static, Result<VideoMetadata, String>> {
        let metadata = self.0.clone();
        Box::pin(async move { Ok(metadata) })
    }
}

struct FixedConvert(u64);
impl ConvertStage for FixedConvert {
    fn convert(
        &self,
        _input: PathBuf,
        _output: PathBuf,
        _metadata: VideoMetadata,
        _video_id: String,
        _progress_tx: ProgressSender,
    ) -> BoxFuture<'static, Result<u64, String>> {
        let size = self.0;
        Box::pin(async move { Ok(size) })
    }
}

/// [`JobRunner`] that drives a real [`ProcessorOrchestrator`] over fake
/// stages -- i.e. the same wiring `worker::PipelineRunner` does for
/// production stages, but for tests. This is what actually ties #10
/// (scheduling) and #11 (the pipeline) together end-to-end.
struct FakePipelineRunner {
    repository: Arc<Repository>,
    work_dir: PathBuf,
}

impl JobRunner for FakePipelineRunner {
    fn run(&self, video: Video) -> BoxFuture<'static, Result<(), String>> {
        let orchestrator = ProcessorOrchestrator::new(
            Arc::new(OkDownload),
            Arc::new(FixedAnalyze(h264_metadata())),
            Arc::new(FixedConvert(42_000)),
            Arc::new(OkUpload),
            self.repository.clone(),
            self.work_dir.clone(),
            "av1".to_string(),
            RetryConfig {
                max_attempts: 1,
                base_delay_secs: 0,
                max_delay_secs: 0,
            },
            test_progress_tx(),
        );
        Box::pin(async move { orchestrator.process_one(video).await })
    }
}

/// Scenario: "submit video -> assigned to worker -> processing -> done",
/// but exercised through the real `JobQueue` (not by calling
/// `ProcessorOrchestrator::process_one` directly, which is already covered
/// by `worker::processor`'s own unit tests) so the claim/schedule/run
/// wiring itself is what's under test.
#[tokio::test]
async fn test_full_local_pipeline_moves_pending_video_to_done() {
    let (dir, repo) = repo_with_pending(1).await;
    let runner = Arc::new(FakePipelineRunner {
        repository: repo.clone(),
        work_dir: dir.path().join("work"),
    });
    let queue = Arc::new(JobQueue::new(
        repo.clone(),
        runner,
        "worker-1".to_string(),
        2,
    ));

    let handle = tokio::spawn({
        let queue = queue.clone();
        async move { queue.process().await }
    });
    // `process()` never returns on its own; give it time to claim + finish
    // the one pending job, then stop it.
    tokio::time::sleep(Duration::from_millis(300)).await;
    handle.abort();

    let video = repo.get_video("v0").await.unwrap().unwrap();
    assert_eq!(video.status, "done");
    assert_eq!(video.converted_size_bytes, Some(42_000));
}

/// Scenario: "multiple workers coordinating (no double-processing)". Two
/// independent `JobQueue`s (distinct `instance_id`s, as two worker
/// processes would have) pointed at the *same* DB race to claim and process
/// several pending videos concurrently. `Repository::claim_next_pending`'s
/// atomicity (#10) must guarantee each video is run exactly once, no matter
/// which queue happens to grab it.
#[tokio::test]
async fn test_two_job_queues_never_double_process_the_same_video() {
    const TOTAL_VIDEOS: usize = 12;
    let (dir, repo) = repo_with_pending(TOTAL_VIDEOS).await;

    let run_counts: Arc<StdMutex<HashMap<String, u32>>> = Arc::new(StdMutex::new(HashMap::new()));

    struct CountingRunner {
        repository: Arc<Repository>,
        counts: Arc<StdMutex<HashMap<String, u32>>>,
    }
    impl JobRunner for CountingRunner {
        fn run(&self, video: Video) -> BoxFuture<'static, Result<(), String>> {
            let repository = self.repository.clone();
            let counts = self.counts.clone();
            Box::pin(async move {
                *counts.lock().unwrap().entry(video.id.clone()).or_insert(0) += 1;
                // A small delay widens the window in which a real race
                // (were `claim_next_pending` not atomic) would show up as a
                // count > 1.
                tokio::time::sleep(Duration::from_millis(20)).await;
                repository
                    .update_video_result(&video.id, 0, "done")
                    .await
                    .map_err(|e| e.to_string())
            })
        }
    }

    let runner = Arc::new(CountingRunner {
        repository: repo.clone(),
        counts: run_counts.clone(),
    });

    let queue_a = Arc::new(JobQueue::new(
        repo.clone(),
        runner.clone(),
        "worker-a".to_string(),
        3,
    ));
    let queue_b = Arc::new(JobQueue::new(
        repo.clone(),
        runner.clone(),
        "worker-b".to_string(),
        3,
    ));

    let handle_a = tokio::spawn({
        let q = queue_a.clone();
        async move { q.process().await }
    });
    let handle_b = tokio::spawn({
        let q = queue_b.clone();
        async move { q.process().await }
    });

    tokio::time::sleep(Duration::from_millis(800)).await;
    handle_a.abort();
    handle_b.abort();
    let _ = dir; // keep tempdir alive for the duration of the test

    let snapshot = run_counts.lock().unwrap().clone();
    assert_eq!(
        snapshot.len(),
        TOTAL_VIDEOS,
        "every video should have been processed exactly once by *someone*"
    );
    for (video_id, count) in snapshot.iter() {
        assert_eq!(*count, 1, "video {video_id} was processed {count} times");
    }

    let still_pending = repo.get_pending_videos(100).await.unwrap();
    assert!(still_pending.is_empty());
}

/// Scenario: "worker crash -> job reassigned". A job claimed by
/// `worker-1` that never reports back (simulating a crash) is reclaimed by
/// `Repository::reclaim_stale_jobs` (#17) once its `claimed_at` is old
/// enough, and can then be picked up and completed by a different worker's
/// `JobQueue`.
#[tokio::test]
async fn test_stale_job_from_crashed_worker_is_reclaimed_and_completed_by_another_worker() {
    let (dir, repo) = repo_with_pending(1).await;

    // worker-1 claims it, then "crashes" -- no further status update ever
    // arrives for this job.
    let claimed = repo.claim_next_pending("worker-1").await.unwrap().unwrap();
    assert_eq!(claimed.status, "downloading");

    // Backdate `claimed_at` via a second connection to the same DB file
    // (WAL mode, #4, allows concurrent connections) to simulate enough wall
    // time having passed without a real sleep. Formatted to match SQLite's
    // own `CURRENT_TIMESTAMP` rendering, exactly as
    // `Repository::reclaim_stale_jobs`'s own comment explains is required
    // for the comparison to be meaningful.
    let raw_conn = DbConnection::new(dir.path().join("test.db")).await.unwrap();
    sqlx::query("UPDATE videos SET claimed_at = ? WHERE id = ?")
        .bind(
            (Utc::now() - chrono::Duration::hours(1))
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
        )
        .bind(&claimed.id)
        .execute(raw_conn.pool())
        .await
        .unwrap();

    let reclaimed = repo
        .reclaim_stale_jobs(Utc::now() - chrono::Duration::minutes(30))
        .await
        .unwrap();
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].status, "pending");

    // A second worker's JobQueue now picks it up and completes it.
    let runner = Arc::new(FakePipelineRunner {
        repository: repo.clone(),
        work_dir: dir.path().join("work"),
    });
    let queue = Arc::new(JobQueue::new(
        repo.clone(),
        runner,
        "worker-2".to_string(),
        1,
    ));
    let handle = tokio::spawn({
        let queue = queue.clone();
        async move { queue.process().await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    handle.abort();

    let video = repo.get_video(&claimed.id).await.unwrap().unwrap();
    assert_eq!(video.status, "done");
    assert_eq!(video.instance_id.as_deref(), Some("worker-2"));
}

/// Scenario: "network partition -> graceful degradation" at the local
/// pipeline level (the master/coordinator side of this scenario is covered
/// by `api_integration.rs`'s `test_coordinator_survives_master_being_unreachable`):
/// a video already claimed locally must keep being processable by
/// `JobQueue` regardless of what's happening (or not happening) with master
/// sync, since the two run as fully independent tasks in `main.rs`.
#[tokio::test]
async fn test_job_queue_keeps_processing_independent_of_sync_state() {
    let (dir, repo) = repo_with_pending(1).await;
    let runner = Arc::new(FakePipelineRunner {
        repository: repo.clone(),
        work_dir: dir.path().join("work"),
    });
    let queue = Arc::new(JobQueue::new(
        repo.clone(),
        runner,
        "worker-1".to_string(),
        1,
    ));

    // No coordinator/master involved at all here -- this is the point: the
    // queue neither knows nor cares whether a master is reachable.
    let handle = tokio::spawn({
        let queue = queue.clone();
        async move { queue.process().await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    handle.abort();

    let video = repo.get_video("v0").await.unwrap().unwrap();
    assert_eq!(video.status, "done");
}

// --- Real ffmpeg/ffprobe end-to-end (ignored by default, see README) ---

struct LocalFsDownload {
    root: PathBuf,
}
impl DownloadStage for LocalFsDownload {
    fn download(
        &self,
        remote: String,
        local: PathBuf,
        _video_id: String,
        _progress_tx: ProgressSender,
    ) -> BoxFuture<'static, Result<(), String>> {
        let source = self.root.join(&remote);
        Box::pin(async move {
            tokio::fs::copy(&source, &local)
                .await
                .map(|_| ())
                .map_err(|e| format!("fake NAS download failed: {e}"))
        })
    }
}

struct LocalFsUpload {
    root: PathBuf,
}
impl UploadStage for LocalFsUpload {
    fn upload(
        &self,
        local: PathBuf,
        remote: String,
        _video_id: String,
        _progress_tx: ProgressSender,
    ) -> BoxFuture<'static, Result<(), String>> {
        let dest = self.root.join(&remote);
        Box::pin(async move {
            if let Some(parent) = dest.parent() {
                let _ = tokio::fs::create_dir_all(parent).await;
            }
            tokio::fs::copy(&local, &dest)
                .await
                .map(|_| ())
                .map_err(|e| format!("fake NAS upload failed: {e}"))
        })
    }
}

/// End-to-end pipeline test #16's plan calls for ("insérer une vidéo
/// factice en DB + fichier vidéo généré via `ffmpeg -f lavfi`, lancer
/// `Processor::process_one`, vérifier `status = 'done'`"), using the *real*
/// #6/#7 (`RealAnalyzeStage`/`RealConvertStage`, real ffprobe/ffmpeg
/// processes) but a local-filesystem stand-in for the NAS (#8/#9), per that
/// same plan's explicit guidance to avoid a real NAS/Samba dependency in
/// tests. Requires `ffmpeg`/`ffprobe` on `PATH`; run explicitly with
/// `cargo test -- --ignored` (see README).
#[tokio::test]
#[ignore]
async fn test_full_pipeline_with_real_ffmpeg_analyze_and_convert() {
    use trein_video::config::ConversionConfig;
    use trein_video::video::VideoConverter;
    use trein_video::worker::processor::{RealAnalyzeStage, RealConvertStage};

    let dir = tempfile::tempdir().unwrap();
    let nas_dir = dir.path().join("fake_nas");
    tokio::fs::create_dir_all(&nas_dir).await.unwrap();

    let input_name = "sample.mp4";
    let status = tokio::process::Command::new("ffmpeg")
        .args(["-f", "lavfi", "-i", "testsrc=duration=1:size=64x64:rate=1"])
        .args(["-c:v", "libx264", "-y"])
        .arg(nas_dir.join(input_name))
        .status()
        .await
        .expect("ffmpeg must be installed to run this test");
    assert!(status.success());

    let db_conn = DbConnection::new(dir.path().join("db.sqlite"))
        .await
        .unwrap();
    let repo = Arc::new(Repository::new(db_conn.pool().clone()));
    let mut video = pending_video("v1");
    video.file_path = input_name.to_string();
    repo.insert_video(&video).await.unwrap();
    let claimed = repo.claim_next_pending("worker-1").await.unwrap().unwrap();

    let converter = Arc::new(VideoConverter::new("ultrafast".to_string(), 40));
    let conversion_config = ConversionConfig {
        codec: "av1".to_string(),
        preset: "ultrafast".to_string(),
        crf: 40,
        max_parallel_jobs: 1,
    };

    let orchestrator = ProcessorOrchestrator::new(
        Arc::new(LocalFsDownload {
            root: nas_dir.clone(),
        }),
        Arc::new(RealAnalyzeStage),
        Arc::new(RealConvertStage {
            converter,
            conversion_config,
        }),
        Arc::new(LocalFsUpload { root: nas_dir }),
        repo.clone(),
        dir.path().join("work"),
        "av1".to_string(),
        RetryConfig::default(),
        test_progress_tx(),
    );

    let result = orchestrator.process_one(claimed).await;
    assert!(result.is_ok(), "{result:?}");

    let updated = repo.get_video("v1").await.unwrap().unwrap();
    assert_eq!(updated.status, "done");
    assert!(updated.converted_size_bytes.unwrap_or(0) > 0);
}
