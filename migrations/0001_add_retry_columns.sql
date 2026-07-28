-- #17: retry bookkeeping. `attempts`/`last_retry_time` are brand new columns
-- that no database (however old) has ever had, so a plain `ADD COLUMN` is
-- always safe here regardless of the database's history -- unlike
-- `claimed_at`, which predates real migrations and is instead handled by a
-- one-off compatibility check in `db::connection::DbConnection::new` before
-- migrations run (see that function's comments).
ALTER TABLE videos ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE videos ADD COLUMN last_retry_time DATETIME;
