use crate::config::{ConversionConfig, RetryConfig};
use crate::db::{Repository, Video};
use crate::discord::DiscordNotifier;
use crate::nas::SmbClient;
use crate::progress::ProgressSender;
use crate::video::VideoConverter;
use crate::worker::processor::ProcessorOrchestrator;
use chrono::Utc;
use futures::future::BoxFuture;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{error, info, Instrument};

/// Interval to wait before re-polling the queue after finding it empty, so
/// an idle worker doesn't spin-loop hammering SQLite.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(3);

/// How often the background reclaim task (#17) checks for orphaned jobs.
const RECLAIM_CHECK_INTERVAL: Duration = Duration::from_secs(60);

/// A job claimed longer than this ago and still non-terminal is assumed to
/// belong to a crashed worker and is reclaimed back to `pending` (#17).
const STALE_JOB_THRESHOLD: chrono::Duration = chrono::Duration::minutes(30);

/// Anything that knows how to fully process one claimed [`Video`] (download
/// -> convert -> upload -> persist result). Abstracted behind a trait so
/// `JobQueue`'s scheduling/concurrency logic — the actual subject of this
/// ticket — can be unit-tested with a fake runner, independent of whether
/// ffmpeg/smbclient are installed on the machine running the tests.
pub trait JobRunner: Send + Sync {
    fn run(&self, video: Video) -> BoxFuture<'static, Result<(), String>>;
}

/// Polls the DB for pending jobs and runs up to `max_parallel_jobs` of them
/// concurrently.
///
/// Concurrency is capped with a [`Semaphore`] rather than a hand-rolled
/// counter. Claiming is delegated to [`Repository::claim_next_pending`], the
/// atomic `UPDATE ... RETURNING` that replaces the racy `get_pending_videos`
/// + `update_video_status` pair (see that method's docs for why the old
///   approach could hand the same video to two workers at once).
pub struct JobQueue {
    repository: Arc<Repository>,
    runner: Arc<dyn JobRunner>,
    instance_id: String,
    max_parallel_jobs: usize,
    poll_interval: Duration,
}

impl JobQueue {
    pub fn new(
        repository: Arc<Repository>,
        runner: Arc<dyn JobRunner>,
        instance_id: String,
        max_parallel_jobs: usize,
    ) -> Self {
        Self {
            repository,
            runner,
            instance_id,
            // `config::validate_config` is expected to reject 0, but guard
            // here too so a bad config can never deadlock the worker on a
            // `Semaphore::new(0)` instead of failing loudly at startup.
            max_parallel_jobs: max_parallel_jobs.max(1),
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }

    /// Run forever: claim + spawn jobs while capacity allows, sleeping
    /// `poll_interval` whenever the queue is (temporarily) empty. Also
    /// spawns a background task that periodically reclaims jobs orphaned by
    /// a crashed worker (#17) so this or another worker's queue can pick
    /// them back up.
    pub async fn process(&self) -> anyhow::Result<()> {
        info!("JobQueue::process - creating semaphore");
        let semaphore = Arc::new(Semaphore::new(self.max_parallel_jobs));
        info!("JobQueue::process - spawning reclaim task");
        tokio::spawn(reclaim_stale_jobs_periodically(self.repository.clone()));
        info!("JobQueue::process - starting main loop");

        loop {
            info!("JobQueue::process - claiming next job");
            match self.claim_and_spawn(&semaphore).await {
                Ok(true) => {
                    info!("JobQueue::process - job spawned");
                }
                Ok(false) => {
                    info!("JobQueue::process - queue empty, sleeping");
                    tokio::time::sleep(self.poll_interval).await;
                }
                Err(e) => {
                    error!(error = %e, "failed to claim next pending video");
                    tokio::time::sleep(self.poll_interval).await;
                }
            }
        }
    }

    /// Try to claim one pending video and spawn a task to process it.
    /// Returns `Ok(true)` if a job was claimed and spawned, `Ok(false)` if
    /// the queue was empty. Split out from `process()` so tests can drive a
    /// bounded number of iterations instead of an infinite loop.
    async fn claim_and_spawn(&self, semaphore: &Arc<Semaphore>) -> anyhow::Result<bool> {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore is never closed for the lifetime of the worker");

        match self.repository.claim_next_pending(&self.instance_id).await {
            Ok(Some(video)) => {
                let repository = self.repository.clone();
                let runner = self.runner.clone();
                let video_id = video.id.clone();
                let span = tracing::info_span!("job", video_id = %video_id);

                tokio::spawn(
                    async move {
                        // Held until this task completes (success, error, or
                        // panics and unwinds), then dropped, freeing the slot.
                        let _permit = permit;
                        match runner.run(video).await {
                            Ok(()) => info!("job completed successfully"),
                            Err(message) => {
                                error!(error = %message, "job failed");
                                if let Err(db_err) =
                                    repository.fail_video(&video_id, &message).await
                                {
                                    error!(error = %db_err, "failed to persist job failure to DB");
                                }
                            }
                        }
                    }
                    .instrument(span),
                );
                Ok(true)
            }
            Ok(None) => {
                drop(permit);
                Ok(false)
            }
            Err(e) => {
                drop(permit);
                Err(e)
            }
        }
    }
}

/// Real [`JobRunner`]: delegates to [`ProcessorOrchestrator`] (#11), which
/// implements download (#8) -> analyze (#6) -> convert (#7, with #19's
/// content-aware parameters) -> upload (#9), persisting progress/results at
/// each step, retrying transient download/upload failures per `retry_config`
/// (#17).
pub struct PipelineRunner {
    pub smb_client: Arc<SmbClient>,
    pub converter: Arc<VideoConverter>,
    pub repository: Arc<Repository>,
    /// Local scratch directory; each job gets `work_dir/<video_id>/`.
    pub work_dir: PathBuf,
    pub conversion_config: ConversionConfig,
    pub retry_config: RetryConfig,
    /// Progress event sender (#20), cloned into a fresh
    /// [`ProcessorOrchestrator`] for each job -- `mpsc::Sender` is cheaply
    /// cloneable and shares the same underlying bounded channel/consumer.
    pub progress_tx: ProgressSender,
    /// Discord webhook notifier: `None` when Discord notifications are
    /// disabled or unconfigured (see `config::DiscordConfig`), in which case
    /// [`ProcessorOrchestrator`] simply skips sending anything. Cloning an
    /// `Arc` is cheap, so this is cloned into a fresh `ProcessorOrchestrator`
    /// for each job like the other fields.
    pub discord_notifier: Option<Arc<DiscordNotifier>>,
}

impl JobRunner for PipelineRunner {
    fn run(&self, video: Video) -> BoxFuture<'static, Result<(), String>> {
        let orchestrator = ProcessorOrchestrator::from_real(
            self.smb_client.clone(),
            self.converter.clone(),
            self.repository.clone(),
            self.work_dir.clone(),
            self.conversion_config.clone(),
            self.retry_config.clone(),
            self.progress_tx.clone(),
            self.discord_notifier.clone(),
        );
        Box::pin(async move { orchestrator.process_one(video).await })
    }
}

/// Background task (#17): every [`RECLAIM_CHECK_INTERVAL`], reset any job
/// claimed more than [`STALE_JOB_THRESHOLD`] ago and still non-terminal back
/// to `pending`, on the assumption that the worker holding it crashed
/// without ever reporting a final status. Runs for the lifetime of the
/// worker process, alongside (not blocking) the main claim/spawn loop.
async fn reclaim_stale_jobs_periodically(repository: Arc<Repository>) {
    let mut interval = tokio::time::interval(RECLAIM_CHECK_INTERVAL);
    // The first tick fires immediately; skip it so we don't reclaim before
    // the worker has even had a chance to claim anything.
    interval.tick().await;
    loop {
        interval.tick().await;
        let cutoff = Utc::now() - STALE_JOB_THRESHOLD;
        match repository.reclaim_stale_jobs(cutoff).await {
            Ok(reclaimed) if !reclaimed.is_empty() => {
                info!(
                    count = reclaimed.len(),
                    "reclaimed jobs orphaned by a crashed worker"
                );
            }
            Ok(_) => {}
            Err(e) => error!(error = %e, "failed to reclaim stale jobs"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::connection::DbConnection;
    use chrono::Utc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn make_video(id: &str) -> Video {
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
            created_at: Utc::now(),
            updated_at: Utc::now(),
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
            repo.insert_video(&make_video(&format!("v{i}")))
                .await
                .unwrap();
        }
        (dir, Arc::new(repo))
    }

    /// Runner that tracks concurrency: increments a counter on entry,
    /// records the running max, sleeps briefly, then decrements.
    struct ConcurrencyTrackingRunner {
        current: Arc<AtomicUsize>,
        max_seen: Arc<AtomicUsize>,
    }

    impl JobRunner for ConcurrencyTrackingRunner {
        fn run(&self, _video: Video) -> BoxFuture<'static, Result<(), String>> {
            let current = self.current.clone();
            let max_seen = self.max_seen.clone();
            Box::pin(async move {
                let now = current.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                current.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn test_job_queue_respects_max_parallel_jobs() {
        const TOTAL_VIDEOS: usize = 10;
        const MAX_PARALLEL: usize = 2;

        let (_dir, repo) = repo_with_pending(TOTAL_VIDEOS).await;
        let current = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let runner = Arc::new(ConcurrencyTrackingRunner {
            current: current.clone(),
            max_seen: max_seen.clone(),
        });

        let queue = JobQueue::new(repo.clone(), runner, "worker-1".to_string(), MAX_PARALLEL);
        let semaphore = Arc::new(Semaphore::new(MAX_PARALLEL));

        // Drive claim_and_spawn directly instead of the infinite `process()`
        // loop, so the test can terminate once the queue is empty.
        loop {
            match queue.claim_and_spawn(&semaphore).await {
                Ok(true) => {}
                Ok(false) => break,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }

        // Give the last spawned tasks time to finish.
        tokio::time::sleep(Duration::from_millis(300)).await;

        assert!(
            max_seen.load(Ordering::SeqCst) <= MAX_PARALLEL,
            "max concurrent jobs {} exceeded limit {}",
            max_seen.load(Ordering::SeqCst),
            MAX_PARALLEL
        );
        assert_eq!(current.load(Ordering::SeqCst), 0);
    }

    struct PanickingRunner;
    impl JobRunner for PanickingRunner {
        fn run(&self, _video: Video) -> BoxFuture<'static, Result<(), String>> {
            Box::pin(async move { panic!("deliberate panic for test") })
        }
    }

    #[tokio::test]
    async fn test_panicking_job_does_not_crash_worker_and_releases_permit() {
        let (_dir, repo) = repo_with_pending(1).await;
        let runner = Arc::new(PanickingRunner);
        let queue = JobQueue::new(repo.clone(), runner, "worker-1".to_string(), 1);
        let semaphore = Arc::new(Semaphore::new(1));

        let claimed = queue.claim_and_spawn(&semaphore).await.unwrap();
        assert!(claimed);

        // The permit must be released once the panicking task unwinds, even
        // though nobody awaited its JoinHandle.
        let acquired = tokio::time::timeout(Duration::from_secs(2), semaphore.acquire()).await;
        assert!(
            acquired.is_ok(),
            "semaphore permit was not released after the spawned task panicked"
        );
    }

    struct FailingRunner;
    impl JobRunner for FailingRunner {
        fn run(&self, _video: Video) -> BoxFuture<'static, Result<(), String>> {
            Box::pin(async move { Err("simulated failure".to_string()) })
        }
    }

    #[tokio::test]
    async fn test_failed_job_marks_video_failed_in_db() {
        let (_dir, repo) = repo_with_pending(1).await;
        let runner = Arc::new(FailingRunner);
        let queue = JobQueue::new(repo.clone(), runner, "worker-1".to_string(), 1);
        let semaphore = Arc::new(Semaphore::new(1));

        queue.claim_and_spawn(&semaphore).await.unwrap();
        // Wait for the spawned task to run and persist the failure.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let video = repo.get_video("v0").await.unwrap().unwrap();
        assert_eq!(video.status, "failed");
        assert_eq!(video.error_message.as_deref(), Some("simulated failure"));
    }
}
