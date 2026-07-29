-- #31: indexes for the queries the job queue actually runs at scale. There
-- is no literal `job_queue` table -- the queue is implemented as a
-- status/instance_id/claimed_at state machine over `videos` (see
-- `Repository::claim_next_pending`, `get_pending_videos`,
-- `reclaim_stale_jobs`) -- so these target the `videos` columns those
-- queries filter/sort by, plus the one FK column SQLite never indexes
-- automatically.
--
-- `CREATE INDEX IF NOT EXISTS`, consistent with the cautious idempotent
-- style already used in `0000_init.sql`: re-running this migration, or
-- applying it against a database that already has these indexes some other
-- way (e.g. `db::connection::create_optimized_indexes`, called defensively
-- on every startup -- see that function's doc comment for why it duplicates
-- these statements), must always be a no-op rather than an error.

-- `get_pending_videos`'s `WHERE status = 'pending'` scan, and the
-- `WHERE status = 'pending'` subquery inside `claim_next_pending`.
CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status);

-- Sorting/filtering by date.
CREATE INDEX IF NOT EXISTS idx_videos_created_at ON videos(created_at);

-- Covers `claim_next_pending`'s hot path exactly: `WHERE status = 'pending'
-- ORDER BY created_at LIMIT 1` -- pick the oldest pending job -- without a
-- separate sort step once `status` narrows the row set.
CREATE INDEX IF NOT EXISTS idx_videos_status_created_at ON videos(status, created_at);

-- Worker-specific queries (e.g. "this instance's currently claimed jobs").
-- Leading column `instance_id` also serves plain `WHERE instance_id = ?`
-- lookups via the standard leftmost-prefix rule, so a separate
-- single-column index on `instance_id` alone isn't needed.
CREATE INDEX IF NOT EXISTS idx_videos_instance_id_status ON videos(instance_id, status);

-- `conversion_logs.video_id` references `videos(id)`, but SQLite does not
-- auto-index foreign key columns the way some other databases do.
CREATE INDEX IF NOT EXISTS idx_conversion_logs_video_id ON conversion_logs(video_id);
