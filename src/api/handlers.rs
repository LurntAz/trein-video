use crate::api::models::{
    ApiResponse, HeartbeatRequest, PendingVideosQuery, StatusUpdateRequest, VideoResponse,
};
use crate::db::{Instance, Repository};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use chrono::Utc;
use std::sync::Arc;
use tracing::{error, warn};

/// Shared application state injected into every handler that needs DB
/// access, via axum's `State` extractor.
pub type SharedRepository = Arc<Repository>;

/// Default number of pending videos returned by `GET /api/videos/pending`
/// when `limit` isn't specified, and the hard ceiling on it regardless of
/// what a (possibly misconfigured) worker requests -- see #14's plan on
/// guarding against an accidental DoS from an unbounded `limit`.
const DEFAULT_PENDING_LIMIT: i64 = 50;
const MAX_PENDING_LIMIT: i64 = 500;

fn ok_response<T: serde::Serialize>(data: T) -> axum::response::Response {
    Json(ApiResponse::ok(data)).into_response()
}

fn error_response(status: StatusCode, message: impl Into<String>) -> axum::response::Response {
    (status, Json(ApiResponse::<()>::err(message))).into_response()
}

pub async fn health_handler() -> &'static str {
    "OK"
}

/// `GET /api/videos/pending?limit=N`
pub async fn get_pending_videos(
    State(repo): State<SharedRepository>,
    Query(query): Query<PendingVideosQuery>,
) -> axum::response::Response {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_PENDING_LIMIT)
        .clamp(1, MAX_PENDING_LIMIT);

    match repo.get_pending_videos(limit).await {
        Ok(videos) => {
            let response: Vec<VideoResponse> =
                videos.into_iter().map(VideoResponse::from).collect();
            ok_response(response)
        }
        Err(e) => {
            error!(error = %e, "failed to list pending videos");
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to list pending videos",
            )
        }
    }
}

/// `GET /api/videos/{id}/status`
pub async fn get_video_status(
    State(repo): State<SharedRepository>,
    Path(id): Path<String>,
) -> axum::response::Response {
    match repo.get_video(&id).await {
        Ok(Some(video)) => ok_response(VideoResponse::from(video)),
        Ok(None) => error_response(StatusCode::NOT_FOUND, format!("video '{id}' not found")),
        Err(e) => {
            error!(error = %e, video_id = %id, "failed to fetch video");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed to fetch video")
        }
    }
}

const VALID_STATUSES: [&str; 6] = [
    "pending",
    "downloading",
    "converting",
    "uploading",
    "done",
    "failed",
];

/// Whitelist of status transitions the pipeline can legitimately produce
/// (mirrors `ProcessorOrchestrator`/`Repository::claim_next_pending` in
/// #10/#11): a worker reporting anything else is either buggy or stale, and
/// must not be trusted to mutate global state (see #14's plan).
fn is_valid_transition(from: &str, to: &str) -> bool {
    if to == "failed" {
        // Any non-terminal state can be failed; a video already `done` or
        // `failed` is terminal and cannot be re-failed.
        return from != "done" && from != "failed";
    }
    matches!(
        (from, to),
        ("pending", "downloading")
            | ("downloading", "converting")
            // `should_convert() == false` short-circuits straight from
            // `downloading` to `done`, skipping convert/upload entirely.
            | ("downloading", "done")
            | ("converting", "uploading")
            | ("uploading", "done")
    )
}

/// `POST /api/videos/{id}/status`
pub async fn update_video_status(
    State(repo): State<SharedRepository>,
    Path(id): Path<String>,
    Json(body): Json<StatusUpdateRequest>,
) -> axum::response::Response {
    if !VALID_STATUSES.contains(&body.status.as_str()) {
        return error_response(
            StatusCode::BAD_REQUEST,
            format!("invalid status '{}'", body.status),
        );
    }

    let video = match repo.get_video(&id).await {
        Ok(Some(v)) => v,
        Ok(None) => {
            return error_response(StatusCode::NOT_FOUND, format!("video '{id}' not found"))
        }
        Err(e) => {
            error!(error = %e, video_id = %id, "failed to fetch video for status update");
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed to fetch video");
        }
    };

    if let Some(existing_instance) = &video.instance_id {
        if existing_instance != &body.instance_id {
            warn!(
                video_id = %id,
                existing = %existing_instance,
                reported = %body.instance_id,
                "status update from unexpected instance rejected"
            );
            return error_response(
                StatusCode::CONFLICT,
                "video is claimed by a different instance",
            );
        }
    }

    if !is_valid_transition(&video.status, &body.status) {
        return error_response(
            StatusCode::BAD_REQUEST,
            format!(
                "invalid status transition '{}' -> '{}'",
                video.status, body.status
            ),
        );
    }

    // The master never claims videos itself (#10's `claim_next_pending` only
    // runs against a worker's own local DB), so this status report is the
    // first and only place its `instance_id` column can get populated.
    // Recording it here (idempotently -- see the method's docs) is what
    // makes the ownership check above meaningful for any *later* report on
    // this same video: without it, `video.instance_id` would stay `None`
    // forever and a second worker could report on (and clobber) a job it
    // was never assigned.
    if let Err(e) = repo
        .set_video_owner_if_unclaimed(&id, &body.instance_id)
        .await
    {
        error!(error = %e, video_id = %id, "failed to record video owner");
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to update video status",
        );
    }

    let write_result = if body.status == "failed" {
        let message = body
            .error_message
            .unwrap_or_else(|| "unspecified failure reported by worker".to_string());
        repo.fail_video(&id, &message).await
    } else if body.status == "done" {
        let size = body
            .converted_size_bytes
            .unwrap_or_else(|| video.converted_size_bytes.unwrap_or(0));
        repo.update_video_result(&id, size, "done").await
    } else {
        repo.update_video_status(&id, &body.status).await
    };

    if let Err(e) = write_result {
        error!(error = %e, video_id = %id, "failed to update video status");
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to update video status",
        );
    }

    match repo.get_video(&id).await {
        Ok(Some(updated)) => ok_response(VideoResponse::from(updated)),
        Ok(None) => error_response(StatusCode::NOT_FOUND, format!("video '{id}' not found")),
        Err(e) => {
            error!(error = %e, video_id = %id, "failed to reload video after status update");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed to reload video")
        }
    }
}

/// `POST /api/instances/heartbeat`
pub async fn heartbeat(
    State(repo): State<SharedRepository>,
    Json(body): Json<HeartbeatRequest>,
) -> axum::response::Response {
    if body.role != "master" && body.role != "worker" {
        return error_response(StatusCode::BAD_REQUEST, "role must be 'master' or 'worker'");
    }

    let instance = Instance {
        id: body.id,
        role: body.role,
        api_url: body.api_url,
        last_heartbeat: Utc::now(),
        is_alive: true,
    };

    match repo.register_instance(&instance).await {
        Ok(()) => ok_response(instance),
        Err(e) => {
            error!(error = %e, "failed to register heartbeat");
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to register heartbeat",
            )
        }
    }
}

/// `GET /api/instances`
pub async fn get_instances(State(repo): State<SharedRepository>) -> axum::response::Response {
    match repo.get_instances().await {
        Ok(instances) => ok_response(instances),
        Err(e) => {
            error!(error = %e, "failed to list instances");
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to list instances",
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::connection::DbConnection;
    use crate::db::Video;
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::{get, post};
    use axum::Router;
    use chrono::Utc;
    use serde_json::{json, Value};
    use tower::util::ServiceExt;

    async fn test_repo() -> (tempfile::TempDir, Arc<Repository>) {
        let dir = tempfile::tempdir().unwrap();
        let conn = DbConnection::new(dir.path().join("test.db")).await.unwrap();
        (dir, Arc::new(Repository::new(conn.pool().clone())))
    }

    fn make_video(id: &str, status: &str, instance_id: Option<&str>) -> Video {
        Video {
            id: id.to_string(),
            file_path: format!("/videos/{id}.mp4"),
            status: status.to_string(),
            original_codec: None,
            original_bitrate_kbps: None,
            original_size_bytes: None,
            converted_size_bytes: None,
            instance_id: instance_id.map(|s| s.to_string()),
            error_message: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            claimed_at: None,
            attempts: 0,
            last_retry_time: None,
        }
    }

    /// Insert `id` as `pending` and then actually claim it for
    /// `instance_id` via `claim_next_pending` -- `Repository::insert_video`
    /// deliberately never writes `instance_id` (only claiming does), so
    /// tests that need a video with a real claiming instance in the DB must
    /// go through this rather than constructing a `Video` with
    /// `instance_id: Some(..)` and inserting it directly.
    async fn insert_and_claim(repo: &Repository, id: &str, instance_id: &str) {
        repo.insert_video(&make_video(id, "pending", None))
            .await
            .unwrap();
        let claimed = repo.claim_next_pending(instance_id).await.unwrap().unwrap();
        assert_eq!(claimed.id, id);
    }

    fn router(repo: Arc<Repository>) -> Router {
        Router::new()
            .route("/health", get(health_handler))
            .route("/api/videos/pending", get(get_pending_videos))
            .route(
                "/api/videos/:id/status",
                get(get_video_status).post(update_video_status),
            )
            .route("/api/instances/heartbeat", post(heartbeat))
            .route("/api/instances", get(get_instances))
            .with_state(repo)
    }

    async fn body_json(response: axum::response::Response) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn test_health_endpoint_returns_ok() {
        let (_dir, repo) = test_repo().await;
        let app = router(repo);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_pending_videos_returns_inserted_videos() {
        let (_dir, repo) = test_repo().await;
        repo.insert_video(&make_video("v1", "pending", None))
            .await
            .unwrap();
        repo.insert_video(&make_video("v2", "pending", None))
            .await
            .unwrap();
        let app = router(repo);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/videos/pending")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert!(body["success"].as_bool().unwrap());
        assert_eq!(body["data"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_get_pending_videos_clamps_limit() {
        let (_dir, repo) = test_repo().await;
        let app = router(repo);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/videos/pending?limit=999999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // A huge limit must not error out -- it's silently clamped.
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_video_status_found() {
        let (_dir, repo) = test_repo().await;
        repo.insert_video(&make_video("v1", "pending", None))
            .await
            .unwrap();
        let app = router(repo);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/videos/v1/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["data"]["id"], "v1");
    }

    #[tokio::test]
    async fn test_get_video_status_missing_returns_404() {
        let (_dir, repo) = test_repo().await;
        let app = router(repo);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/videos/does-not-exist/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = body_json(response).await;
        assert!(!body["success"].as_bool().unwrap());
    }

    fn post_json(uri: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    #[tokio::test]
    async fn test_update_video_status_valid_transition_succeeds() {
        let (_dir, repo) = test_repo().await;
        repo.insert_video(&make_video("v1", "pending", None))
            .await
            .unwrap();
        let app = router(repo.clone());

        let response = app
            .oneshot(post_json(
                "/api/videos/v1/status",
                json!({"status": "downloading", "instance_id": "worker-1"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let video = repo.get_video("v1").await.unwrap().unwrap();
        assert_eq!(video.status, "downloading");
    }

    #[tokio::test]
    async fn test_update_video_status_invalid_transition_returns_400() {
        let (_dir, repo) = test_repo().await;
        repo.insert_video(&make_video("v1", "done", Some("worker-1")))
            .await
            .unwrap();
        let app = router(repo);

        let response = app
            .oneshot(post_json(
                "/api/videos/v1/status",
                json!({"status": "pending", "instance_id": "worker-1"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_update_video_status_unknown_status_returns_400() {
        let (_dir, repo) = test_repo().await;
        repo.insert_video(&make_video("v1", "pending", None))
            .await
            .unwrap();
        let app = router(repo);

        let response = app
            .oneshot(post_json(
                "/api/videos/v1/status",
                json!({"status": "not-a-real-status", "instance_id": "worker-1"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_update_video_status_missing_video_returns_404() {
        let (_dir, repo) = test_repo().await;
        let app = router(repo);

        let response = app
            .oneshot(post_json(
                "/api/videos/does-not-exist/status",
                json!({"status": "downloading", "instance_id": "worker-1"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_update_video_status_first_report_claims_ownership_for_later_requests() {
        // Regression test: the master never runs `claim_next_pending` (#10)
        // itself, so a video synced down via `GET /api/videos/pending`
        // (#15) has `instance_id: None` on the master until some worker
        // reports progress on it. That first report must record the
        // reporting instance as the owner, or a second worker's later
        // report on the same video would never be rejected (see
        // `Repository::set_video_owner_if_unclaimed`'s docs).
        let (_dir, repo) = test_repo().await;
        repo.insert_video(&make_video("v1", "pending", None))
            .await
            .unwrap();
        let app = router(repo.clone());

        let first = app
            .clone()
            .oneshot(post_json(
                "/api/videos/v1/status",
                json!({"status": "downloading", "instance_id": "worker-1"}),
            ))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let video = repo.get_video("v1").await.unwrap().unwrap();
        assert_eq!(video.instance_id.as_deref(), Some("worker-1"));

        let second = app
            .oneshot(post_json(
                "/api/videos/v1/status",
                json!({"status": "converting", "instance_id": "worker-2"}),
            ))
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_update_video_status_wrong_instance_returns_409() {
        let (_dir, repo) = test_repo().await;
        insert_and_claim(&repo, "v1", "worker-1").await;
        let app = router(repo.clone());

        let response = app
            .oneshot(post_json(
                "/api/videos/v1/status",
                json!({"status": "converting", "instance_id": "worker-2"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);

        // Must not have been mutated by the rejected request.
        let video = repo.get_video("v1").await.unwrap().unwrap();
        assert_eq!(video.status, "downloading");
    }

    #[tokio::test]
    async fn test_update_video_status_failed_persists_error_message() {
        let (_dir, repo) = test_repo().await;
        insert_and_claim(&repo, "v1", "worker-1").await;
        let app = router(repo.clone());

        let response = app
            .oneshot(post_json(
                "/api/videos/v1/status",
                json!({"status": "failed", "instance_id": "worker-1", "error_message": "NAS unreachable"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let video = repo.get_video("v1").await.unwrap().unwrap();
        assert_eq!(video.status, "failed");
        assert_eq!(video.error_message.as_deref(), Some("NAS unreachable"));
    }

    #[tokio::test]
    async fn test_heartbeat_registers_instance() {
        let (_dir, repo) = test_repo().await;
        let app = router(repo.clone());

        let response = app
            .oneshot(post_json(
                "/api/instances/heartbeat",
                json!({"id": "worker-1", "role": "worker", "api_url": "https://192.168.1.20:8000"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let instances = repo.get_instances().await.unwrap();
        assert_eq!(instances.len(), 1);
        assert_eq!(instances[0].id, "worker-1");
    }

    #[tokio::test]
    async fn test_heartbeat_invalid_role_returns_400() {
        let (_dir, repo) = test_repo().await;
        let app = router(repo);

        let response = app
            .oneshot(post_json(
                "/api/instances/heartbeat",
                json!({"id": "worker-1", "role": "supervisor", "api_url": "https://x"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_get_instances_lists_registered_instances() {
        let (_dir, repo) = test_repo().await;
        let app = router(repo.clone());

        app.clone()
            .oneshot(post_json(
                "/api/instances/heartbeat",
                json!({"id": "master-1", "role": "master", "api_url": "https://192.168.1.10:8000"}),
            ))
            .await
            .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/instances")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["data"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_is_valid_transition_allows_expected_pipeline_transitions() {
        assert!(is_valid_transition("pending", "downloading"));
        assert!(is_valid_transition("downloading", "converting"));
        assert!(is_valid_transition("downloading", "done"));
        assert!(is_valid_transition("converting", "uploading"));
        assert!(is_valid_transition("uploading", "done"));
        assert!(is_valid_transition("downloading", "failed"));
        assert!(is_valid_transition("converting", "failed"));
    }

    #[test]
    fn test_is_valid_transition_rejects_terminal_state_changes() {
        assert!(!is_valid_transition("done", "pending"));
        assert!(!is_valid_transition("done", "failed"));
        assert!(!is_valid_transition("failed", "pending"));
        assert!(!is_valid_transition("failed", "failed"));
    }

    #[test]
    fn test_is_valid_transition_rejects_skipping_stages() {
        assert!(!is_valid_transition("pending", "converting"));
        assert!(!is_valid_transition("pending", "done"));
        assert!(!is_valid_transition("uploading", "converting"));
    }
}
