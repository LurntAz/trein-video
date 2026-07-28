use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Video {
    pub id: String,
    pub file_path: String,
    pub status: String, // pending, downloading, converting, uploading, done, failed
    pub original_codec: Option<String>,
    pub original_bitrate_kbps: Option<i32>,
    pub original_size_bytes: Option<i64>,
    pub converted_size_bytes: Option<i64>,
    pub instance_id: Option<String>,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// When a worker claimed this job (`claim_next_pending`). Used to detect
    /// jobs orphaned by a crashed worker; reclaiming them is handled by
    /// `Repository::reclaim_stale_jobs` (#17).
    #[serde(default)]
    pub claimed_at: Option<DateTime<Utc>>,
    /// Number of processing attempts made so far, incremented by
    /// `Repository::increment_attempts` each time a retryable error (#17)
    /// forces the pipeline to retry a stage.
    #[serde(default)]
    pub attempts: i32,
    /// Timestamp of the most recent retry, if any (#17).
    #[serde(default)]
    pub last_retry_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Instance {
    pub id: String,
    pub role: String, // master or worker
    pub api_url: String,
    pub last_heartbeat: DateTime<Utc>,
    pub is_alive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ConversionLog {
    pub id: i64,
    pub video_id: String,
    pub instance_id: String,
    pub action: String, // download_start, convert_start, convert_done, upload_done
    pub duration_secs: Option<f64>,
    pub notes: Option<String>,
    pub timestamp: DateTime<Utc>,
}
