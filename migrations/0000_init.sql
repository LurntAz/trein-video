-- Baseline schema, regenerated (#17) from what `db::connection::DbConnection`
-- used to create inline via `CREATE TABLE IF NOT EXISTS` before this project
-- adopted real `sqlx migrate` migrations. `IF NOT EXISTS` is kept
-- deliberately here (even though a normal first migration wouldn't need it)
-- so that databases created by that older code -- which already have these
-- three tables, just not tracked in `_sqlx_migrations` -- treat this
-- migration as a no-op instead of failing on "table already exists", and
-- get folded into the new migration history going forward.
CREATE TABLE IF NOT EXISTS videos (
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
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    claimed_at DATETIME
);

CREATE TABLE IF NOT EXISTS instances (
    id TEXT PRIMARY KEY,
    role TEXT CHECK(role IN ('master', 'worker')),
    api_url TEXT,
    last_heartbeat DATETIME,
    is_alive BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS conversion_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT REFERENCES videos(id),
    instance_id TEXT,
    action TEXT,
    duration_secs REAL,
    notes TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);
