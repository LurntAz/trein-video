use anyhow::{Context, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::path::Path;

/// Tables that must exist once migrations have run successfully. `videos`,
/// `instances`, and `conversion_logs` come from `0000_init.sql`;
/// `_sqlx_migrations` is created and maintained by `sqlx::migrate!` itself,
/// so its absence means migrations never actually ran against this
/// connection at all (as opposed to a partially-applied schema).
///
/// There is no `job_queue` table: the job queue (#10, #17) is implemented as
/// a `status`/`instance_id`/`claimed_at` state machine over `videos` rather
/// than a separate table -- see `Repository::claim_next_pending`.
const EXPECTED_TABLES: &[&str] = &["videos", "instances", "conversion_logs", "_sqlx_migrations"];

pub struct DbConnection {
    pool: SqlitePool,
}

impl DbConnection {
    pub async fn new<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        // Ensure parent directory exists
        if let Some(parent) = Path::new(db_path.as_ref()).parent() {
            std::fs::create_dir_all(parent)?;
        }

        // `create_if_missing` defaults to `false` in sqlx; without it,
        // connecting to a DB file that doesn't exist yet (e.g. first boot)
        // fails with "unable to open database file" instead of creating it.
        let options = SqliteConnectOptions::new()
            .filename(db_path.as_ref())
            .create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await?;

        // WAL mode allows concurrent readers while a writer holds the lock,
        // which we need once multiple worker tasks/instances hit the same
        // SQLite file (job queue, #10). `busy_timeout` makes writers block
        // and retry for a bit instead of failing immediately with
        // `SQLITE_BUSY` under contention.
        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(&pool)
            .await?;
        sqlx::query("PRAGMA busy_timeout=5000")
            .execute(&pool)
            .await?;

        // Legacy compatibility shim (#17): databases created by versions of
        // this code that predate real `sqlx migrate` migrations (everything
        // before #17) already have `videos`/`instances`/`conversion_logs`
        // tables, just not tracked in `_sqlx_migrations`. `0000_init.sql`
        // uses `CREATE TABLE IF NOT EXISTS`, which is a no-op against such a
        // table -- including its `claimed_at` column, which is *not* part of
        // the very oldest schema (pre-#10) and would therefore never get
        // added by migrations alone. Patch that one column in directly,
        // before migrations run, but only if the table already exists: a
        // brand-new database has no `videos` table yet, and
        // `0000_init.sql` already creates one with `claimed_at` included.
        let videos_table_exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'videos'",
        )
        .fetch_one(&pool)
        .await?;

        if videos_table_exists > 0 {
            let has_claimed_at: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM pragma_table_info('videos') WHERE name = 'claimed_at'",
            )
            .fetch_one(&pool)
            .await?;

            if has_claimed_at == 0 {
                sqlx::query("ALTER TABLE videos ADD COLUMN claimed_at DATETIME")
                    .execute(&pool)
                    .await?;
            }
        }

        // Run migrations (#17: `attempts`/`last_retry_time`; #31: indexes on
        // top of the baseline schema regenerated in `0000_init.sql`).
        sqlx::migrate!("./migrations").run(&pool).await?;

        // #31: don't just trust that `migrate!` produced the schema the rest
        // of this codebase assumes -- verify it explicitly, with an error
        // that names exactly what's missing, and fail startup loudly rather
        // than surfacing a confusing "no such table" error the first time a
        // query runs. Belt-and-suspenders on top of `create_optimized_indexes`
        // for the same reason: every instance (master and worker alike) goes
        // through `DbConnection::new`, so this is the one place guaranteed to
        // run before this connection serves any query.
        verify_db_schema(&pool).await?;
        create_optimized_indexes(&pool).await?;

        Ok(Self { pool })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

/// Verify that every table this codebase depends on actually exists,
/// post-migration. Returns a single error listing every missing table by
/// name (not just the first one hit) so a human fixing a broken deployment
/// database knows exactly what's wrong without bisecting migrations by hand.
///
/// See [`EXPECTED_TABLES`] for why `job_queue` is not among them.
pub async fn verify_db_schema(pool: &SqlitePool) -> Result<()> {
    let mut missing = Vec::new();

    for table in EXPECTED_TABLES {
        let exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(table)
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to query sqlite_master for table '{table}'"))?;

        if exists == 0 {
            missing.push(*table);
        }
    }

    if !missing.is_empty() {
        anyhow::bail!(
            "database schema verification failed: missing table(s) [{}] after running \
             migrations -- the database file may be corrupt, from an incompatible version, \
             or migrations did not run. Check `migrations/` against the current schema and \
             the `_sqlx_migrations` table's applied-version history before proceeding.",
            missing.join(", ")
        );
    }

    Ok(())
}

/// Create every index this codebase relies on for query performance, and
/// refresh the query planner's statistics so it actually uses them.
///
/// The same `CREATE INDEX IF NOT EXISTS` statements already live in
/// `migrations/0002_add_indexes.sql`, which is the source of truth for
/// schema history; this function re-asserts them at every startup as a
/// defensive no-op layer (all `IF NOT EXISTS`, so this is always cheap and
/// never errors on a database that already has them) rather than a
/// competing source of truth. That way a database whose migration history
/// is out of sync for any reason -- e.g. `_sqlx_migrations` shows 0002
/// applied but the index was manually dropped -- still ends up with the
/// indexes this build's queries expect.
///
/// `job_queue.status` / `job_queue.instance_id` from the original ticket
/// request map onto `videos.status` / `videos.instance_id`: see
/// [`EXPECTED_TABLES`]'s doc comment for why there is no separate
/// `job_queue` table.
pub async fn create_optimized_indexes(pool: &SqlitePool) -> Result<()> {
    let statements: &[(&str, &str)] = &[
        (
            "idx_videos_status",
            "CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status)",
        ),
        (
            "idx_videos_created_at",
            "CREATE INDEX IF NOT EXISTS idx_videos_created_at ON videos(created_at)",
        ),
        (
            "idx_videos_status_created_at",
            "CREATE INDEX IF NOT EXISTS idx_videos_status_created_at ON videos(status, created_at)",
        ),
        (
            "idx_videos_instance_id_status",
            "CREATE INDEX IF NOT EXISTS idx_videos_instance_id_status ON videos(instance_id, status)",
        ),
        (
            "idx_conversion_logs_video_id",
            "CREATE INDEX IF NOT EXISTS idx_conversion_logs_video_id ON conversion_logs(video_id)",
        ),
    ];

    for (name, sql) in statements {
        sqlx::query(sql)
            .execute(pool)
            .await
            .with_context(|| format!("failed to create index '{name}'"))?;
    }

    // Refreshes SQLite's query planner statistics (equivalent to a
    // lightweight `ANALYZE`) so the indexes just created are actually
    // eligible to be picked by the planner immediately, rather than waiting
    // for it to notice on its own. Cheap and safe to run on every startup.
    sqlx::query("PRAGMA optimize")
        .execute(pool)
        .await
        .context("failed to run PRAGMA optimize")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_new_enables_wal_mode() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = DbConnection::new(&db_path).await.unwrap();

        let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(conn.pool())
            .await
            .unwrap();
        assert_eq!(mode.to_lowercase(), "wal");
    }

    #[tokio::test]
    async fn test_new_adds_claimed_at_column() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = DbConnection::new(&db_path).await.unwrap();

        let column_exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pragma_table_info('videos') WHERE name = 'claimed_at'",
        )
        .fetch_one(conn.pool())
        .await
        .unwrap();
        assert_eq!(column_exists, 1);
    }

    #[tokio::test]
    async fn test_new_is_idempotent() {
        // Opening the same DB file twice (e.g. process restart) must not
        // fail on the ALTER TABLE / CREATE TABLE statements.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let _conn1 = DbConnection::new(&db_path).await.unwrap();
        let _conn2 = DbConnection::new(&db_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_new_adds_retry_columns() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = DbConnection::new(&db_path).await.unwrap();

        for column in ["attempts", "last_retry_time"] {
            let column_exists: i64 = sqlx::query_scalar(&format!(
                "SELECT COUNT(*) FROM pragma_table_info('videos') WHERE name = '{column}'"
            ))
            .fetch_one(conn.pool())
            .await
            .unwrap();
            assert_eq!(column_exists, 1, "missing column {column}");
        }
    }

    /// Regression test for #17's migration-compatibility requirement:
    /// databases created by the pre-migrations code (literally the original
    /// `CREATE TABLE` statements this test recreates by hand, with no
    /// `claimed_at`/`attempts`/`last_retry_time` and no `_sqlx_migrations`
    /// bookkeeping at all) must come out the other side of
    /// `DbConnection::new` with the full current schema, and -- critically
    /// -- without losing any pre-existing data.
    #[tokio::test]
    async fn test_migrates_pre_existing_legacy_schema_without_data_loss() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("legacy.db");

        {
            let options = SqliteConnectOptions::new()
                .filename(&db_path)
                .create_if_missing(true);
            let legacy_pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(options)
                .await
                .unwrap();

            // The literal original schema, pre-#10/#17: no `claimed_at`, no
            // `attempts`/`last_retry_time`, no `_sqlx_migrations` table.
            sqlx::query(
                "CREATE TABLE videos (
                    id TEXT PRIMARY KEY,
                    file_path TEXT UNIQUE NOT NULL,
                    status TEXT CHECK(status IN ('pending', 'downloading', 'converting', 'uploading', 'done', 'failed')),
                    original_codec TEXT,
                    original_bitrate_kbps INTEGER,
                    original_size_bytes BIGINT,
                    converted_size_bytes BIGINT,
                    instance_id TEXT,
                    error_message TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )",
            )
            .execute(&legacy_pool)
            .await
            .unwrap();
            sqlx::query(
                "CREATE TABLE instances (
                    id TEXT PRIMARY KEY,
                    role TEXT CHECK(role IN ('master', 'worker')),
                    api_url TEXT,
                    last_heartbeat DATETIME,
                    is_alive BOOLEAN DEFAULT TRUE
                )",
            )
            .execute(&legacy_pool)
            .await
            .unwrap();
            sqlx::query(
                "CREATE TABLE conversion_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT REFERENCES videos(id),
                    instance_id TEXT,
                    action TEXT,
                    duration_secs REAL,
                    notes TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )",
            )
            .execute(&legacy_pool)
            .await
            .unwrap();

            sqlx::query(
                "INSERT INTO videos (id, file_path, status) VALUES ('legacy-1', '/videos/legacy.mp4', 'pending')",
            )
            .execute(&legacy_pool)
            .await
            .unwrap();

            legacy_pool.close().await;
        }

        // Opening it through the current code must migrate it forward.
        let conn = DbConnection::new(&db_path).await.unwrap();

        for column in ["claimed_at", "attempts", "last_retry_time"] {
            let column_exists: i64 = sqlx::query_scalar(&format!(
                "SELECT COUNT(*) FROM pragma_table_info('videos') WHERE name = '{column}'"
            ))
            .fetch_one(conn.pool())
            .await
            .unwrap();
            assert_eq!(column_exists, 1, "missing column {column} after migration");
        }

        let (status, attempts): (String, i64) =
            sqlx::query_as("SELECT status, attempts FROM videos WHERE id = 'legacy-1'")
                .fetch_one(conn.pool())
                .await
                .unwrap();
        assert_eq!(status, "pending", "pre-existing row must survive migration");
        assert_eq!(attempts, 0, "new column must default to 0 on old rows");
    }

    #[tokio::test]
    async fn test_verify_db_schema_passes_after_normal_startup() {
        // `DbConnection::new` already calls `verify_db_schema` internally
        // and would have failed startup if this didn't hold, but assert it
        // directly too so a regression here fails with a specific message
        // instead of just "DbConnection::new returned Err".
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = DbConnection::new(&db_path).await.unwrap();

        verify_db_schema(conn.pool()).await.unwrap();
    }

    #[tokio::test]
    async fn test_verify_db_schema_reports_missing_table_by_name() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = DbConnection::new(&db_path).await.unwrap();

        // Simulate a corrupted/incomplete database by dropping a table
        // `DbConnection::new` already successfully created.
        sqlx::query("DROP TABLE conversion_logs")
            .execute(conn.pool())
            .await
            .unwrap();

        let err = verify_db_schema(conn.pool()).await.unwrap_err();
        let message = format!("{err}");
        assert!(
            message.contains("conversion_logs"),
            "error message should name the missing table, got: {message}"
        );
    }

    #[tokio::test]
    async fn test_create_optimized_indexes_creates_expected_indexes() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = DbConnection::new(&db_path).await.unwrap();

        for index in [
            "idx_videos_status",
            "idx_videos_created_at",
            "idx_videos_status_created_at",
            "idx_videos_instance_id_status",
            "idx_conversion_logs_video_id",
        ] {
            let exists: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?",
            )
            .bind(index)
            .fetch_one(conn.pool())
            .await
            .unwrap();
            assert_eq!(exists, 1, "missing index {index}");
        }
    }

    #[tokio::test]
    async fn test_create_optimized_indexes_is_idempotent() {
        // `DbConnection::new` already ran this once; calling it again
        // directly (e.g. simulating a second startup against the same pool)
        // must not error on "index already exists".
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = DbConnection::new(&db_path).await.unwrap();

        create_optimized_indexes(conn.pool()).await.unwrap();
        create_optimized_indexes(conn.pool()).await.unwrap();
    }
}
