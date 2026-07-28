use crate::db::Video;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Uniform envelope for every JSON response this API returns, so clients
/// (the worker's sync client, #15) have one error-handling code path
/// regardless of endpoint.
#[derive(Debug, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn ok(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn err(message: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(message.into()),
        }
    }
}

/// API-facing view of a [`Video`], decoupled from the DB row shape.
/// Deliberately includes more than just `id`/`status` -- notably
/// `file_path` -- so the worker (#15) knows what to download once it picks
/// a job up from `GET /api/videos/pending`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VideoResponse {
    pub id: String,
    pub file_path: String,
    pub status: String,
    pub original_codec: Option<String>,
    pub original_bitrate_kbps: Option<i32>,
    pub original_size_bytes: Option<i64>,
    pub converted_size_bytes: Option<i64>,
    pub instance_id: Option<String>,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Number of processing attempts made so far (#17) -- surfaced so a
    /// worker/operator can tell a job that's still on its first try apart
    /// from one that's already been retried several times.
    #[serde(default)]
    pub attempts: i32,
}

impl From<Video> for VideoResponse {
    fn from(v: Video) -> Self {
        Self {
            id: v.id,
            file_path: v.file_path,
            status: v.status,
            original_codec: v.original_codec,
            original_bitrate_kbps: v.original_bitrate_kbps,
            original_size_bytes: v.original_size_bytes,
            converted_size_bytes: v.converted_size_bytes,
            instance_id: v.instance_id,
            error_message: v.error_message,
            created_at: v.created_at,
            updated_at: v.updated_at,
            attempts: v.attempts,
        }
    }
}

/// Body of `GET /api/videos/pending`'s query string.
#[derive(Debug, Deserialize)]
pub struct PendingVideosQuery {
    pub limit: Option<i64>,
}

/// Body of `POST /api/videos/{id}/status`, sent by a worker to report its
/// progress/outcome for a job it holds. `instance_id` is required and
/// checked against the video's claiming instance in the DB, so a worker
/// can't (even accidentally) clobber another worker's job.
/// Also `Serialize` so the worker-side sync coordinator (#15) can build the
/// exact same payload shape it sends to `POST /api/videos/{id}/status`
/// without duplicating an equivalent struct.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusUpdateRequest {
    pub status: String,
    pub instance_id: String,
    #[serde(default)]
    pub error_message: Option<String>,
    #[serde(default)]
    pub converted_size_bytes: Option<i64>,
}

/// Body of `POST /api/instances/heartbeat`. Also `Serialize` so the worker
/// sync coordinator (#15) can build this request directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub id: String,
    pub role: String,
    pub api_url: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_video_response_from_video_preserves_all_fields() {
        let now = Utc::now();
        let video = Video {
            id: "v1".to_string(),
            file_path: "/videos/v1.mp4".to_string(),
            status: "done".to_string(),
            original_codec: Some("h264".to_string()),
            original_bitrate_kbps: Some(5000),
            original_size_bytes: Some(1_000_000),
            converted_size_bytes: Some(500_000),
            instance_id: Some("worker-1".to_string()),
            error_message: None,
            created_at: now,
            updated_at: now,
            claimed_at: Some(now),
            attempts: 2,
            last_retry_time: Some(now),
        };
        let response = VideoResponse::from(video);
        assert_eq!(response.id, "v1");
        assert_eq!(response.file_path, "/videos/v1.mp4");
        assert_eq!(response.original_codec.as_deref(), Some("h264"));
        assert_eq!(response.converted_size_bytes, Some(500_000));
        assert_eq!(response.attempts, 2);
    }

    #[test]
    fn test_api_response_ok_and_err() {
        let ok: ApiResponse<u32> = ApiResponse::ok(42);
        assert!(ok.success);
        assert_eq!(ok.data, Some(42));
        assert!(ok.error.is_none());

        let err: ApiResponse<u32> = ApiResponse::err("boom");
        assert!(!err.success);
        assert!(err.data.is_none());
        assert_eq!(err.error.as_deref(), Some("boom"));
    }
}
