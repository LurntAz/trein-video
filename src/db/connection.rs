use anyhow::Result;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::path::Path;

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

        // Run migrations (#17: `attempts`/`last_retry_time` on top of the
        // baseline schema regenerated in `0000_init.sql`).
        sqlx::migrate!("./migrations").run(&pool).await?;

        Ok(Self { pool })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
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
}
