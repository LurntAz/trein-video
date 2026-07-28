use super::models::{ConversionLog, Instance, Video};
use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::{Row, SqlitePool};

pub struct Repository {
    pool: SqlitePool,
}

impl Repository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    // Video operations
    pub async fn insert_video(&self, video: &Video) -> Result<()> {
        sqlx::query(
            "INSERT INTO videos (id, file_path, status, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&video.id)
        .bind(&video.file_path)
        .bind(&video.status)
        .bind(video.created_at)
        .bind(video.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_video(&self, id: &str) -> Result<Option<Video>> {
        let video = sqlx::query_as::<_, Video>("SELECT * FROM videos WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(video)
    }

    pub async fn get_pending_videos(&self, limit: i64) -> Result<Vec<Video>> {
        let videos =
            sqlx::query_as::<_, Video>("SELECT * FROM videos WHERE status = 'pending' LIMIT ?")
                .bind(limit)
                .fetch_all(&self.pool)
                .await?;
        Ok(videos)
    }

    /// Get all video IDs for efficient bulk operations (e.g., discovery).
    pub async fn get_all_video_ids(&self) -> Result<Vec<String>> {
        let rows = sqlx::query("SELECT id FROM videos")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.iter().map(|row| row.get::<String, _>(0)).collect())
    }

    pub async fn update_video_status(&self, id: &str, status: &str) -> Result<()> {
        sqlx::query("UPDATE videos SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?")
            .bind(status)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Record `instance_id` as the owner of `id`, but only if no owner is
    /// recorded yet (`COALESCE` is a no-op once `instance_id` is already
    /// set). Used by the master's `POST /api/videos/{id}/status` handler
    /// (#14) the first time any worker reports progress on a video it only
    /// knows about via `GET /api/videos/pending` (#15) -- unlike
    /// `claim_next_pending` (#10), the master never claims videos itself, so
    /// this is the only place its own `instance_id` column ever gets
    /// populated. Doing so is what makes the handler's existing
    /// wrong-instance-gets-409 check meaningful instead of dead code: without
    /// an owner ever being recorded, a second worker's request would always
    /// see `instance_id: None` and never be rejected.
    pub async fn set_video_owner_if_unclaimed(&self, id: &str, instance_id: &str) -> Result<()> {
        sqlx::query("UPDATE videos SET instance_id = COALESCE(instance_id, ?) WHERE id = ?")
            .bind(instance_id)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Atomically claim the oldest `pending` video for `instance_id`,
    /// flipping it to `downloading` in the same statement.
    ///
    /// This replaces the racy `get_pending_videos()` + `update_video_status()`
    /// pair: two workers (or two tasks in the same worker) calling this
    /// concurrently can never both receive the same video, because the row
    /// selection and the status flip happen as a single atomic
    /// `UPDATE ... WHERE id = (SELECT ...) RETURNING *` statement — SQLite
    /// serializes writers, so the second caller's `SELECT` subquery re-runs
    /// against the already-updated table and picks a different row (or
    /// `None` if there isn't one).
    pub async fn claim_next_pending(&self, instance_id: &str) -> Result<Option<Video>> {
        let video = sqlx::query_as::<_, Video>(
            "UPDATE videos
             SET status = 'downloading', instance_id = ?, claimed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
             WHERE id = (
                 SELECT id FROM videos WHERE status = 'pending' ORDER BY created_at LIMIT 1
             )
             RETURNING *",
        )
        .bind(instance_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(video)
    }

    /// Persist the metadata extracted by `VideoAnalyzer::analyze()` after a
    /// successful download.
    pub async fn update_video_metadata(
        &self,
        id: &str,
        codec: &str,
        bitrate_kbps: i32,
        size_bytes: i64,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE videos
             SET original_codec = ?, original_bitrate_kbps = ?, original_size_bytes = ?, updated_at = CURRENT_TIMESTAMP
             WHERE id = ?",
        )
        .bind(codec)
        .bind(bitrate_kbps)
        .bind(size_bytes)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Record the outcome of a finished conversion/upload: the real output
    /// size (from `VideoConverter::convert_to_*`) and the final status.
    pub async fn update_video_result(
        &self,
        id: &str,
        converted_size_bytes: i64,
        status: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE videos
             SET converted_size_bytes = ?, status = ?, updated_at = CURRENT_TIMESTAMP
             WHERE id = ?",
        )
        .bind(converted_size_bytes)
        .bind(status)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Mark a video as failed with an explanatory message. Never panics the
    /// caller — used from job-queue error handlers where a DB write failure
    /// should be logged, not propagated as a second panic.
    pub async fn fail_video(&self, id: &str, error_message: &str) -> Result<()> {
        sqlx::query(
            "UPDATE videos
             SET status = 'failed', error_message = ?, updated_at = CURRENT_TIMESTAMP
             WHERE id = ?",
        )
        .bind(error_message)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Increment `attempts` and stamp `last_retry_time` for a video about to
    /// be retried after a transient error (#17), returning the new attempt
    /// count so the caller can decide whether it has hit its configured
    /// ceiling.
    pub async fn increment_attempts(&self, id: &str) -> Result<i32> {
        let attempts: i32 = sqlx::query_scalar(
            "UPDATE videos
             SET attempts = attempts + 1, last_retry_time = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
             WHERE id = ?
             RETURNING attempts",
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await?;
        Ok(attempts)
    }

    /// Record an error message against a video without changing its status
    /// -- used when a retryable error is logged but the pipeline is about to
    /// retry rather than fail the job outright. Contrast with
    /// [`Repository::fail_video`], which also flips `status` to `'failed'`.
    pub async fn update_video_error(&self, id: &str, message: &str) -> Result<()> {
        sqlx::query(
            "UPDATE videos SET error_message = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        )
        .bind(message)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Reclaim jobs left claimed (`downloading`/`converting`/`uploading`)
    /// whose `claimed_at` predates `older_than`, presumably orphaned by a
    /// worker that crashed mid-job (#17): resets them to `pending` with no
    /// claiming instance so any worker's `claim_next_pending` (#10) can pick
    /// them back up.
    ///
    /// Implemented as a single atomic `UPDATE ... RETURNING`, the same
    /// pattern as `claim_next_pending`, so two queue instances calling this
    /// concurrently can never both reclaim (and thus double-process) the
    /// same stale job -- SQLite serializes writers, so the second caller's
    /// `WHERE` clause re-evaluates against the already-updated rows.
    pub async fn reclaim_stale_jobs(&self, older_than: DateTime<Utc>) -> Result<Vec<Video>> {
        // `claimed_at` is always written by `claim_next_pending` via the SQL
        // literal `CURRENT_TIMESTAMP`, which SQLite renders as
        // `YYYY-MM-DD HH:MM:SS` (space separator, no fractional seconds, no
        // timezone suffix). sqlx's chrono binding for `DateTime<Utc>`
        // renders as RFC3339 (`T` separator, fractional seconds, `+00:00`
        // suffix) instead -- a *different* string format. SQLite compares
        // `TEXT` columns byte-for-byte, and `' ' < 'T'` in ASCII, so binding
        // `older_than` directly would make every `claimed_at < ?` comparison
        // spuriously true (or otherwise nonsensical) regardless of the
        // actual chronological order. Format it to match `CURRENT_TIMESTAMP`
        // exactly instead, so the comparison is a same-format lexicographic
        // (and therefore chronological) one.
        let older_than_sqlite = older_than.format("%Y-%m-%d %H:%M:%S").to_string();
        let videos = sqlx::query_as::<_, Video>(
            "UPDATE videos
             SET status = 'pending', instance_id = NULL, claimed_at = NULL, updated_at = CURRENT_TIMESTAMP
             WHERE status IN ('downloading', 'converting', 'uploading')
               AND claimed_at IS NOT NULL
               AND claimed_at < ?
             RETURNING *",
        )
        .bind(older_than_sqlite)
        .fetch_all(&self.pool)
        .await?;
        Ok(videos)
    }

    // Instance operations
    pub async fn register_instance(&self, instance: &Instance) -> Result<()> {
        sqlx::query(
            "INSERT OR REPLACE INTO instances (id, role, api_url, last_heartbeat, is_alive)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&instance.id)
        .bind(&instance.role)
        .bind(&instance.api_url)
        .bind(instance.last_heartbeat)
        .bind(instance.is_alive)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_instances(&self) -> Result<Vec<Instance>> {
        let instances = sqlx::query_as::<_, Instance>("SELECT * FROM instances")
            .fetch_all(&self.pool)
            .await?;
        Ok(instances)
    }

    // Conversion log operations
    pub async fn insert_log(&self, log: &ConversionLog) -> Result<()> {
        sqlx::query(
            "INSERT INTO conversion_logs (video_id, instance_id, action, duration_secs, notes, timestamp)
             VALUES (?, ?, ?, ?, ?, ?)"
        )
        .bind(&log.video_id)
        .bind(&log.instance_id)
        .bind(&log.action)
        .bind(log.duration_secs)
        .bind(&log.notes)
        .bind(log.timestamp)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::connection::DbConnection;
    use chrono::Utc;
    use std::sync::Arc;

    async fn setup_repo_with_pending(count: usize) -> (tempfile::TempDir, Repository) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = DbConnection::new(&db_path).await.unwrap();
        let repo = Repository::new(conn.pool().clone());

        for i in 0..count {
            let video = Video {
                id: format!("video-{i}"),
                file_path: format!("/videos/video-{i}.mp4"),
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
            };
            repo.insert_video(&video).await.unwrap();
        }

        (dir, repo)
    }

    #[tokio::test]
    async fn test_claim_next_pending_returns_one_video() {
        let (_dir, repo) = setup_repo_with_pending(1).await;
        let claimed = repo.claim_next_pending("worker-1").await.unwrap();
        assert!(claimed.is_some());
        let video = claimed.unwrap();
        assert_eq!(video.status, "downloading");
        assert_eq!(video.instance_id.as_deref(), Some("worker-1"));
        assert!(video.claimed_at.is_some());
    }

    #[tokio::test]
    async fn test_claim_next_pending_empty_queue_returns_none() {
        let (_dir, repo) = setup_repo_with_pending(0).await;
        let claimed = repo.claim_next_pending("worker-1").await.unwrap();
        assert!(claimed.is_none());
    }

    #[tokio::test]
    async fn test_claim_next_pending_never_double_claims_under_concurrency() {
        // Regression test for the race condition described in #10: two
        // callers racing `get_pending_videos` + `update_video_status`
        // separately could both grab the same row. `claim_next_pending`
        // must never let that happen, even with many concurrent callers.
        const TOTAL_VIDEOS: usize = 20;
        let (_dir, repo) = setup_repo_with_pending(TOTAL_VIDEOS).await;
        let repo = Arc::new(repo);

        let mut handles = Vec::new();
        for i in 0..TOTAL_VIDEOS {
            let repo = repo.clone();
            handles.push(tokio::spawn(async move {
                repo.claim_next_pending(&format!("worker-{i}")).await
            }));
        }

        let mut claimed_ids = std::collections::HashSet::new();
        for handle in handles {
            if let Some(video) = handle.await.unwrap().unwrap() {
                // insert() returns false if the id was already present.
                assert!(
                    claimed_ids.insert(video.id.clone()),
                    "video {} was claimed more than once",
                    video.id
                );
            }
        }
        assert_eq!(claimed_ids.len(), TOTAL_VIDEOS);

        // No pending videos should remain.
        let remaining = repo.get_pending_videos(100).await.unwrap();
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn test_update_video_metadata_and_result() {
        let (_dir, repo) = setup_repo_with_pending(1).await;
        repo.update_video_metadata("video-0", "h264", 5000, 1_000_000)
            .await
            .unwrap();
        let video = repo.get_video("video-0").await.unwrap().unwrap();
        assert_eq!(video.original_codec.as_deref(), Some("h264"));
        assert_eq!(video.original_bitrate_kbps, Some(5000));

        repo.update_video_result("video-0", 500_000, "done")
            .await
            .unwrap();
        let video = repo.get_video("video-0").await.unwrap().unwrap();
        assert_eq!(video.converted_size_bytes, Some(500_000));
        assert_eq!(video.status, "done");
    }

    #[tokio::test]
    async fn test_fail_video_sets_error_message() {
        let (_dir, repo) = setup_repo_with_pending(1).await;
        repo.fail_video("video-0", "NAS unreachable").await.unwrap();
        let video = repo.get_video("video-0").await.unwrap().unwrap();
        assert_eq!(video.status, "failed");
        assert_eq!(video.error_message.as_deref(), Some("NAS unreachable"));
    }

    #[tokio::test]
    async fn test_increment_attempts_returns_running_count_and_stamps_retry_time() {
        let (_dir, repo) = setup_repo_with_pending(1).await;

        let first = repo.increment_attempts("video-0").await.unwrap();
        assert_eq!(first, 1);
        let second = repo.increment_attempts("video-0").await.unwrap();
        assert_eq!(second, 2);

        let video = repo.get_video("video-0").await.unwrap().unwrap();
        assert_eq!(video.attempts, 2);
        assert!(video.last_retry_time.is_some());
    }

    #[tokio::test]
    async fn test_update_video_error_does_not_change_status() {
        let (_dir, repo) = setup_repo_with_pending(1).await;
        repo.update_video_error("video-0", "transient: connection timeout")
            .await
            .unwrap();
        let video = repo.get_video("video-0").await.unwrap().unwrap();
        assert_eq!(video.status, "pending");
        assert_eq!(
            video.error_message.as_deref(),
            Some("transient: connection timeout")
        );
    }

    #[tokio::test]
    async fn test_set_video_owner_if_unclaimed_sets_owner_once() {
        let (_dir, repo) = setup_repo_with_pending(1).await;
        repo.set_video_owner_if_unclaimed("video-0", "worker-1")
            .await
            .unwrap();
        let video = repo.get_video("video-0").await.unwrap().unwrap();
        assert_eq!(video.instance_id.as_deref(), Some("worker-1"));
    }

    #[tokio::test]
    async fn test_set_video_owner_if_unclaimed_does_not_overwrite_existing_owner() {
        let (_dir, repo) = setup_repo_with_pending(1).await;
        repo.set_video_owner_if_unclaimed("video-0", "worker-1")
            .await
            .unwrap();
        repo.set_video_owner_if_unclaimed("video-0", "worker-2")
            .await
            .unwrap();
        let video = repo.get_video("video-0").await.unwrap().unwrap();
        assert_eq!(
            video.instance_id.as_deref(),
            Some("worker-1"),
            "the first owner must stick"
        );
    }

    #[tokio::test]
    async fn test_reclaim_stale_jobs_resets_orphaned_job_to_pending() {
        let (_dir, repo) = setup_repo_with_pending(1).await;
        let claimed = repo.claim_next_pending("worker-1").await.unwrap().unwrap();
        assert_eq!(claimed.status, "downloading");

        // Simulate the worker having crashed a long time ago by backdating
        // `claimed_at` directly -- there is no public API to do this, since
        // in production it is always `CURRENT_TIMESTAMP` at claim time.
        // Formatted to match SQLite's own `CURRENT_TIMESTAMP` rendering
        // (space separator, no fractional seconds) rather than sqlx's
        // default RFC3339 `DateTime<Utc>` binding, so the comparison in
        // `reclaim_stale_jobs` -- which formats its own bound parameter the
        // same way for exactly this reason -- compares like with like.
        sqlx::query("UPDATE videos SET claimed_at = ? WHERE id = ?")
            .bind(
                (Utc::now() - chrono::Duration::hours(2))
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string(),
            )
            .bind(&claimed.id)
            .execute(&repo.pool)
            .await
            .unwrap();

        let cutoff = Utc::now() - chrono::Duration::minutes(30);
        let reclaimed = repo.reclaim_stale_jobs(cutoff).await.unwrap();
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].id, claimed.id);
        assert_eq!(reclaimed[0].status, "pending");
        assert!(reclaimed[0].instance_id.is_none());
        assert!(reclaimed[0].claimed_at.is_none());

        // Now claimable again by a different worker.
        let reclaimed_by_other = repo.claim_next_pending("worker-2").await.unwrap().unwrap();
        assert_eq!(reclaimed_by_other.id, claimed.id);
        assert_eq!(reclaimed_by_other.instance_id.as_deref(), Some("worker-2"));
    }

    #[tokio::test]
    async fn test_reclaim_stale_jobs_ignores_recently_claimed_jobs() {
        let (_dir, repo) = setup_repo_with_pending(1).await;
        repo.claim_next_pending("worker-1").await.unwrap().unwrap();

        // `claimed_at` is "now" (set by `claim_next_pending` itself), well
        // after any reasonable cutoff in the past.
        let cutoff = Utc::now() - chrono::Duration::hours(1);
        let reclaimed = repo.reclaim_stale_jobs(cutoff).await.unwrap();
        assert!(
            reclaimed.is_empty(),
            "a job claimed moments ago must not be reclaimed"
        );
    }

    #[tokio::test]
    async fn test_reclaim_stale_jobs_ignores_terminal_statuses() {
        let (_dir, repo) = setup_repo_with_pending(1).await;
        repo.update_video_result("video-0", 100, "done")
            .await
            .unwrap();
        // Even with an old (bogus) claimed_at, a terminal job must never be
        // reset back to pending.
        sqlx::query("UPDATE videos SET claimed_at = ? WHERE id = 'video-0'")
            .bind(
                (Utc::now() - chrono::Duration::days(1))
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string(),
            )
            .execute(&repo.pool)
            .await
            .unwrap();

        let cutoff = Utc::now() - chrono::Duration::minutes(1);
        let reclaimed = repo.reclaim_stale_jobs(cutoff).await.unwrap();
        assert!(reclaimed.is_empty());
    }
}
