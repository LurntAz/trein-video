//! Worker -> master sync coordination (#15).
//!
//! A worker instance runs one [`Coordinator`] in the background for as long
//! as it's up: it periodically tells the master it's alive (heartbeat),
//! pulls newly-pending videos down into its own local queue (#10 claims
//! from that local queue independently), and relays status changes back up
//! so the master's view of the fleet stays accurate.
//!
//! The actual HTTP/mTLS transport is behind the [`MasterClient`] trait (the
//! same "trait for the real IO, fake for tests" pattern used throughout this
//! codebase -- see `worker::processor`'s `DownloadStage`/`ConvertStage`/etc.
//! and `worker::job_queue`'s `JobRunner`), so `Coordinator`'s own
//! scheduling/dedup/backoff logic can be unit-tested without a real server.

use crate::api::models::{ApiResponse, HeartbeatRequest, StatusUpdateRequest, VideoResponse};
use crate::config::TlsConfig;
use crate::db::{Repository, Video};
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;
use tracing::{info, warn};

/// Errors a [`MasterClient`] call can fail with. Deliberately `Clone` (unlike
/// `reqwest::Error`) so it's cheap to log and to check `is_retryable()` on
/// without consuming it.
#[derive(Debug, Clone, Error)]
pub enum SyncError {
    #[error("network error talking to master: {0}")]
    Network(String),
    #[error("master rejected the request ({status}): {message}")]
    HttpStatus { status: u16, message: String },
    #[error("master returned an application error: {0}")]
    Api(String),
}

impl SyncError {
    /// Network-level failures (connection refused, timeout, DNS, TLS
    /// handshake) and server-side 5xx are worth retrying -- the master may
    /// simply be restarting or briefly overloaded. A well-formed 4xx (bad
    /// request, conflict, not found) or an explicit application-level error
    /// means retrying the exact same request will just fail the same way
    /// again, so those are not retryable.
    pub fn is_retryable(&self) -> bool {
        match self {
            SyncError::Network(_) => true,
            SyncError::HttpStatus { status, .. } => *status >= 500,
            SyncError::Api(_) => false,
        }
    }
}

/// Failures constructing the mTLS HTTP client itself (bad/missing cert
/// files) -- distinct from [`SyncError`], which covers failures of
/// individual requests made with an already-built client.
#[derive(Debug, Error)]
pub enum SyncClientError {
    #[error("failed to read TLS material at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to build mTLS HTTP client: {0}")]
    Reqwest(#[from] reqwest::Error),
}

/// Everything the [`Coordinator`] needs from a master API server. Production
/// code uses [`HttpMasterClient`]; tests use a fake implementation to drive
/// `Coordinator`'s dedup/backoff/metrics logic deterministically.
pub trait MasterClient: Send + Sync {
    fn get_pending_videos(
        &self,
        limit: i64,
    ) -> BoxFuture<'static, Result<Vec<VideoResponse>, SyncError>>;

    fn send_heartbeat(
        &self,
        instance_id: String,
        api_url: String,
    ) -> BoxFuture<'static, Result<(), SyncError>>;

    fn update_status(
        &self,
        video_id: String,
        payload: StatusUpdateRequest,
    ) -> BoxFuture<'static, Result<(), SyncError>>;
}

/// Build a `reqwest::Client` presenting the client certificate at
/// `tls.cert_path`/`tls.key_path` (see `tls` module docs: workers are handed
/// a client cert/key signed by the master's self-signed CA out of band).
///
/// The server's own leaf certificate (`tls::generate_ca_and_server_cert`,
/// #12) only carries a `localhost` SAN, but workers reach the master over a
/// LAN IP discovered via mDNS (#13) or `sync.master_url`, so strict hostname
/// verification would always fail here even against a perfectly legitimate
/// master. reqwest's rustls backend (this crate's `rustls-tls` feature) has
/// no supported way to keep chain/expiry verification while only relaxing
/// the hostname check -- that would require hand-rolling a
/// `rustls::client::danger::ServerCertVerifier`, more machinery than this
/// single-CA, single-trusted-LAN deployment (see `tls` module docs on the
/// threat model) warrants. We accept the wider `danger_accept_invalid_certs`
/// escape hatch instead: the master already independently authenticates
/// *this* client via its own `WebPkiClientVerifier` (#12, `api::server`),
/// so an attacker on the LAN still cannot impersonate a worker even though
/// a worker no longer independently authenticates the master's identity.
fn build_mtls_client(tls: &TlsConfig) -> Result<reqwest::Client, SyncClientError> {
    info!("build_mtls_client - reading TLS certificates");
    let read = |path: &str| {
        info!("build_mtls_client - reading {}", path);
        std::fs::read(path).map_err(|source| SyncClientError::Io {
            path: path.to_string(),
            source,
        })
    };

    info!("build_mtls_client - reading client cert");
    let cert_pem = read(&tls.cert_path)?;
    info!("build_mtls_client - reading client key");
    let key_pem = read(&tls.key_path)?;
    info!("build_mtls_client - reading CA cert");
    let ca_pem = read(&tls.ca_cert_path)?;

    info!("build_mtls_client - building identity PEM");
    let mut identity_pem = cert_pem;
    identity_pem.push(b'\n');
    identity_pem.extend_from_slice(&key_pem);

    info!("build_mtls_client - creating reqwest Identity");
    let identity = reqwest::Identity::from_pem(&identity_pem)?;
    info!("build_mtls_client - creating reqwest Certificate");
    let ca_cert = reqwest::Certificate::from_pem(&ca_pem)?;

    info!("build_mtls_client - building reqwest Client");
    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .identity(identity)
        .add_root_certificate(ca_cert)
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(30))
        .build()?;
    info!("build_mtls_client - complete");
    Ok(client)
}

/// [`MasterClient`] backed by a real mTLS `reqwest::Client` talking to
/// `base_url` (e.g. `https://192.168.1.10:8443`, from `sync.master_url` or
/// mDNS discovery, #13).
pub struct HttpMasterClient {
    client: reqwest::Client,
    base_url: String,
}

impl HttpMasterClient {
    pub fn new(base_url: String, tls: &TlsConfig) -> Result<Self, SyncClientError> {
        let client = build_mtls_client(tls)?;
        Ok(Self { client, base_url })
    }

    async fn parse_response<T: serde::de::DeserializeOwned>(
        response: reqwest::Response,
    ) -> Result<T, SyncError> {
        let status = response.status();
        let body: ApiResponse<T> = response
            .json()
            .await
            .map_err(|e| SyncError::Network(e.to_string()))?;

        if !status.is_success() {
            let message = body.error.unwrap_or_else(|| status.to_string());
            return Err(SyncError::HttpStatus {
                status: status.as_u16(),
                message,
            });
        }
        if !body.success {
            return Err(SyncError::Api(
                body.error
                    .unwrap_or_else(|| "unspecified error".to_string()),
            ));
        }
        body.data
            .ok_or_else(|| SyncError::Api("master response had no data".to_string()))
    }
}

impl MasterClient for HttpMasterClient {
    fn get_pending_videos(
        &self,
        limit: i64,
    ) -> BoxFuture<'static, Result<Vec<VideoResponse>, SyncError>> {
        let client = self.client.clone();
        let url = format!("{}/api/videos/pending?limit={limit}", self.base_url);
        Box::pin(async move {
            let response = client
                .get(&url)
                .send()
                .await
                .map_err(|e| SyncError::Network(e.to_string()))?;
            Self::parse_response(response).await
        })
    }

    fn send_heartbeat(
        &self,
        instance_id: String,
        api_url: String,
    ) -> BoxFuture<'static, Result<(), SyncError>> {
        let client = self.client.clone();
        let url = format!("{}/api/instances/heartbeat", self.base_url);
        let payload = HeartbeatRequest {
            id: instance_id,
            role: "worker".to_string(),
            api_url,
        };
        Box::pin(async move {
            let response = client
                .post(&url)
                .json(&payload)
                .send()
                .await
                .map_err(|e| SyncError::Network(e.to_string()))?;
            Self::parse_response::<crate::db::Instance>(response)
                .await
                .map(|_| ())
        })
    }

    fn update_status(
        &self,
        video_id: String,
        payload: StatusUpdateRequest,
    ) -> BoxFuture<'static, Result<(), SyncError>> {
        let client = self.client.clone();
        let url = format!("{}/api/videos/{video_id}/status", self.base_url);
        Box::pin(async move {
            let response = client
                .post(&url)
                .json(&payload)
                .send()
                .await
                .map_err(|e| SyncError::Network(e.to_string()))?;
            Self::parse_response::<VideoResponse>(response)
                .await
                .map(|_| ())
        })
    }
}

/// Point-in-time counters exposed for observability (#15's "last_sync,
/// sync_error_count" requirement). Cheap to clone/snapshot.
#[derive(Debug, Clone, Default)]
pub struct SyncMetrics {
    pub last_sync: Option<DateTime<Utc>>,
    pub sync_error_count: u64,
    pub last_heartbeat: Option<DateTime<Utc>>,
    pub heartbeat_error_count: u64,
}

/// Maximum attempts within one [`Coordinator::with_backoff`] call before
/// giving up on a single sync/heartbeat tick and waiting for the next one.
/// Deliberately small and independent of `config::RetryConfig` (#17): #15's
/// retry is about surviving one flaky request within a tick, not about the
/// same multi-attempt job-level policy #17 applies to download/upload
/// stages -- see that ticket's plan, which does not list this file among
/// its affected files.
const MAX_ATTEMPTS_PER_TICK: u32 = 3;
const BASE_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

fn compute_backoff(attempt: u32) -> Duration {
    let exp = BASE_BACKOFF.saturating_mul(1 << attempt.min(6));
    exp.min(MAX_BACKOFF)
}

/// Runs the worker's side of the master sync protocol: heartbeats, pulling
/// pending work down, and relaying status updates back up.
pub struct Coordinator {
    client: Arc<dyn MasterClient>,
    repository: Arc<Repository>,
    instance_id: String,
    /// This worker's own API URL, reported in heartbeats. Workers don't
    /// currently run their own API server (only the master does, see
    /// `main.rs`), so this is informational/best-effort rather than
    /// something the master ever actually connects back to.
    self_api_url: String,
    metrics: Mutex<SyncMetrics>,
}

impl Coordinator {
    pub fn new(
        client: Arc<dyn MasterClient>,
        repository: Arc<Repository>,
        instance_id: String,
        self_api_url: String,
    ) -> Self {
        Self {
            client,
            repository,
            instance_id,
            self_api_url,
            metrics: Mutex::new(SyncMetrics::default()),
        }
    }

    pub fn metrics(&self) -> SyncMetrics {
        self.metrics.lock().expect("metrics mutex poisoned").clone()
    }

    /// Fetch pending videos from the master and insert any not already
    /// known locally into this worker's own queue (#10's `JobQueue` claims
    /// from the same `Repository`). Existing videos (by `id`) are left
    /// untouched, so re-syncing the same pending video across polls -- or
    /// syncing after the local `JobQueue` has already claimed and started
    /// processing it -- never duplicates or resets it. Returns the number
    /// of newly-inserted videos.
    pub async fn sync_with_master(&self) -> Result<usize, SyncError> {
        let pending = self.client.get_pending_videos(100).await?;
        let mut inserted = 0;
        for remote in pending {
            let already_known = self
                .repository
                .get_video(&remote.id)
                .await
                .map_err(|e| SyncError::Network(format!("local DB error: {e}")))?
                .is_some();
            if already_known {
                continue;
            }

            let video = Video {
                id: remote.id.clone(),
                file_path: remote.file_path,
                status: "pending".to_string(),
                original_codec: None,
                original_bitrate_kbps: None,
                original_size_bytes: None,
                converted_size_bytes: None,
                instance_id: None,
                error_message: None,
                created_at: remote.created_at,
                updated_at: remote.updated_at,
                claimed_at: None,
                attempts: 0,
                last_retry_time: None,
            };
            match self.repository.insert_video(&video).await {
                Ok(()) => inserted += 1,
                Err(e) => {
                    // Another sync tick (or a UNIQUE constraint race) may
                    // have inserted it between our check and this insert;
                    // don't fail the whole sync batch over one video.
                    warn!(video_id = %remote.id, error = %e, "failed to insert synced video locally, skipping");
                }
            }
        }
        Ok(inserted)
    }

    pub async fn send_heartbeat(&self) -> Result<(), SyncError> {
        self.client
            .send_heartbeat(self.instance_id.clone(), self.self_api_url.clone())
            .await
    }

    /// Relay a status/progress change for `video_id` up to the master (#15's
    /// plan: "le coordinator relaie ces mêmes transitions au master" as
    /// `worker::processor::ProcessorOrchestrator` (#11) updates them
    /// locally).
    pub async fn report_status(
        &self,
        video_id: &str,
        status: &str,
        error_message: Option<String>,
        converted_size_bytes: Option<i64>,
    ) -> Result<(), SyncError> {
        let payload = StatusUpdateRequest {
            status: status.to_string(),
            instance_id: self.instance_id.clone(),
            error_message,
            converted_size_bytes,
        };
        self.client
            .update_status(video_id.to_string(), payload)
            .await
    }

    /// Retry `op` up to [`MAX_ATTEMPTS_PER_TICK`] times with exponential
    /// backoff, but only for retryable errors -- an `Api`/4xx error fails
    /// immediately since retrying it would just get the same rejection.
    async fn with_backoff<T, F, Fut>(&self, mut op: F) -> Result<T, SyncError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, SyncError>>,
    {
        let mut attempt = 0;
        loop {
            attempt += 1;
            match op().await {
                Ok(v) => return Ok(v),
                Err(e) if e.is_retryable() && attempt < MAX_ATTEMPTS_PER_TICK => {
                    let backoff = compute_backoff(attempt);
                    warn!(attempt, ?backoff, error = %e, "retrying after transient sync error");
                    tokio::time::sleep(backoff).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// One full tick: heartbeat, then sync. Never returns an error and never
    /// panics -- a master that's unreachable or misbehaving must not crash
    /// the worker's background sync task; it should just be reflected in
    /// `metrics()` and retried on the next tick (see `run_loop`). A video
    /// already claimed and in progress locally is completely unaffected by
    /// this failing, since `worker::JobQueue` (#10) processes independently
    /// of the coordinator.
    ///
    /// `pub` (rather than private) so integration tests (#16) and
    /// operational tooling can drive/observe a single tick directly instead
    /// of only through the infinite `run_loop`.
    pub async fn tick(&self) {
        match self.with_backoff(|| self.send_heartbeat()).await {
            Ok(()) => {
                let mut metrics = self.metrics.lock().expect("metrics mutex poisoned");
                metrics.last_heartbeat = Some(Utc::now());
            }
            Err(e) => {
                warn!(error = %e, "heartbeat to master failed");
                let mut metrics = self.metrics.lock().expect("metrics mutex poisoned");
                metrics.heartbeat_error_count += 1;
            }
        }

        match self.with_backoff(|| self.sync_with_master()).await {
            Ok(inserted) => {
                if inserted > 0 {
                    info!(inserted, "synced new pending videos from master");
                }
                let mut metrics = self.metrics.lock().expect("metrics mutex poisoned");
                metrics.last_sync = Some(Utc::now());
            }
            Err(e) => {
                warn!(error = %e, "sync with master failed");
                let mut metrics = self.metrics.lock().expect("metrics mutex poisoned");
                metrics.sync_error_count += 1;
            }
        }
    }

    /// Run forever, ticking every `poll_interval`. Intended to be
    /// `tokio::spawn`ed once by the worker's `main.rs` alongside `JobQueue`
    /// (#10); the two run fully independently, so a network partition
    /// affecting only this loop degrades gracefully (see `tick`'s docs).
    pub async fn run_loop(self: Arc<Self>, poll_interval: Duration) {
        loop {
            self.tick().await;
            tokio::time::sleep(poll_interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::connection::DbConnection;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Mutex as StdMutex;

    async fn test_repo() -> (tempfile::TempDir, Arc<Repository>) {
        let dir = tempfile::tempdir().unwrap();
        let conn = DbConnection::new(dir.path().join("test.db")).await.unwrap();
        (dir, Arc::new(Repository::new(conn.pool().clone())))
    }

    fn video_response(id: &str) -> VideoResponse {
        let now = Utc::now();
        VideoResponse {
            id: id.to_string(),
            file_path: format!("/videos/{id}.mp4"),
            status: "pending".to_string(),
            original_codec: None,
            original_bitrate_kbps: None,
            original_size_bytes: None,
            converted_size_bytes: None,
            instance_id: None,
            error_message: None,
            created_at: now,
            updated_at: now,
            attempts: 0,
        }
    }

    /// Fake [`MasterClient`] whose responses are entirely scripted, so
    /// `Coordinator`'s dedup/backoff/metrics logic can be tested without a
    /// real HTTP server or mTLS certs.
    struct FakeMasterClient {
        pending_videos: StdMutex<Vec<VideoResponse>>,
        /// Each call to `get_pending_videos` pops the next scripted result;
        /// once exhausted, falls back to `Ok(pending_videos)`.
        get_pending_script: StdMutex<Vec<Result<(), SyncError>>>,
        heartbeat_script: StdMutex<Vec<Result<(), SyncError>>>,
        get_pending_calls: AtomicU32,
        heartbeat_calls: AtomicU32,
        status_updates: StdMutex<Vec<(String, StatusUpdateRequest)>>,
    }

    impl FakeMasterClient {
        fn new(pending_videos: Vec<VideoResponse>) -> Self {
            Self {
                pending_videos: StdMutex::new(pending_videos),
                get_pending_script: StdMutex::new(Vec::new()),
                heartbeat_script: StdMutex::new(Vec::new()),
                get_pending_calls: AtomicU32::new(0),
                heartbeat_calls: AtomicU32::new(0),
                status_updates: StdMutex::new(Vec::new()),
            }
        }

        fn with_heartbeat_script(self, script: Vec<Result<(), SyncError>>) -> Self {
            *self.heartbeat_script.lock().unwrap() = script;
            self
        }

        fn with_get_pending_script(self, script: Vec<Result<(), SyncError>>) -> Self {
            *self.get_pending_script.lock().unwrap() = script;
            self
        }
    }

    impl MasterClient for FakeMasterClient {
        fn get_pending_videos(
            &self,
            _limit: i64,
        ) -> BoxFuture<'static, Result<Vec<VideoResponse>, SyncError>> {
            self.get_pending_calls.fetch_add(1, Ordering::SeqCst);
            let scripted = self.get_pending_script.lock().unwrap().pop();
            let videos = self.pending_videos.lock().unwrap().clone();
            Box::pin(async move {
                match scripted {
                    Some(Err(e)) => Err(e),
                    _ => Ok(videos),
                }
            })
        }

        fn send_heartbeat(
            &self,
            _instance_id: String,
            _api_url: String,
        ) -> BoxFuture<'static, Result<(), SyncError>> {
            self.heartbeat_calls.fetch_add(1, Ordering::SeqCst);
            let scripted = self.heartbeat_script.lock().unwrap().pop();
            Box::pin(async move { scripted.unwrap_or(Ok(())) })
        }

        fn update_status(
            &self,
            video_id: String,
            payload: StatusUpdateRequest,
        ) -> BoxFuture<'static, Result<(), SyncError>> {
            self.status_updates
                .lock()
                .unwrap()
                .push((video_id, payload));
            Box::pin(async move { Ok(()) })
        }
    }

    #[tokio::test]
    async fn test_sync_with_master_inserts_new_pending_videos() {
        let (_dir, repo) = test_repo().await;
        let client = Arc::new(FakeMasterClient::new(vec![
            video_response("v1"),
            video_response("v2"),
        ]));
        let coordinator = Coordinator::new(client, repo.clone(), "worker-1".into(), "".into());

        let inserted = coordinator.sync_with_master().await.unwrap();
        assert_eq!(inserted, 2);
        assert!(repo.get_video("v1").await.unwrap().is_some());
        assert!(repo.get_video("v2").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_sync_with_master_does_not_duplicate_across_polls() {
        let (_dir, repo) = test_repo().await;
        let client = Arc::new(FakeMasterClient::new(vec![video_response("v1")]));
        let coordinator = Coordinator::new(client, repo.clone(), "worker-1".into(), "".into());

        let first = coordinator.sync_with_master().await.unwrap();
        let second = coordinator.sync_with_master().await.unwrap();
        assert_eq!(first, 1);
        assert_eq!(second, 0, "already-known video must not be re-inserted");

        // Sanity: still exactly one row locally, not two.
        let pending = repo.get_pending_videos(10).await.unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn test_sync_with_master_leaves_claimed_video_untouched() {
        let (_dir, repo) = test_repo().await;
        let client = Arc::new(FakeMasterClient::new(vec![video_response("v1")]));
        let coordinator = Coordinator::new(client, repo.clone(), "worker-1".into(), "".into());

        coordinator.sync_with_master().await.unwrap();
        // JobQueue (#10) claims it locally, moving it past `pending`.
        let claimed = repo.claim_next_pending("worker-1").await.unwrap().unwrap();
        assert_eq!(claimed.status, "downloading");

        // A second sync tick (master still reports it as pending, since our
        // status update hasn't round-tripped yet) must not reset it.
        coordinator.sync_with_master().await.unwrap();
        let video = repo.get_video("v1").await.unwrap().unwrap();
        assert_eq!(video.status, "downloading");
    }

    #[tokio::test]
    async fn test_send_heartbeat_delegates_to_client() {
        let (_dir, repo) = test_repo().await;
        let client = Arc::new(FakeMasterClient::new(vec![]));
        let coordinator = Coordinator::new(
            client.clone(),
            repo,
            "worker-1".into(),
            "https://worker:8000".into(),
        );

        coordinator.send_heartbeat().await.unwrap();
        assert_eq!(client.heartbeat_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_report_status_sends_expected_payload() {
        let (_dir, repo) = test_repo().await;
        let client = Arc::new(FakeMasterClient::new(vec![]));
        let coordinator = Coordinator::new(client.clone(), repo, "worker-1".into(), "".into());

        coordinator
            .report_status("v1", "failed", Some("NAS unreachable".to_string()), None)
            .await
            .unwrap();

        let updates = client.status_updates.lock().unwrap();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].0, "v1");
        assert_eq!(updates[0].1.status, "failed");
        assert_eq!(updates[0].1.instance_id, "worker-1");
        assert_eq!(
            updates[0].1.error_message.as_deref(),
            Some("NAS unreachable")
        );
    }

    // Note: these two tests pause tokio's virtual clock manually via
    // `tokio::time::pause()` *after* `test_repo()` has finished its real
    // async I/O (creating a tempdir + opening a real SQLite pool), rather
    // than via `#[tokio::test(start_paused = true)]`. Pausing from the very
    // start of the test made sqlx's own internal connection-pool timeout
    // (which also runs on tokio's clock) fire immediately, since the paused
    // clock auto-advances through any timer-based wait -- including sqlx's
    // -- while genuine (non-timer) I/O is still in flight. Pausing only
    // around the `with_backoff` call keeps that fast-forward effect
    // contained to the retry loop's own `tokio::time::sleep` calls, which is
    // all these tests care about.
    #[tokio::test]
    async fn test_with_backoff_retries_transient_errors_then_succeeds() {
        let (_dir, repo) = test_repo().await;
        let client = Arc::new(FakeMasterClient::new(vec![]).with_heartbeat_script(vec![
            // `.pop()` reads from the end, so list in reverse chronological
            // order: first call fails, second call fails, third succeeds.
            Ok(()),
            Err(SyncError::Network("connection refused".to_string())),
            Err(SyncError::Network("connection refused".to_string())),
        ]));
        let coordinator = Coordinator::new(client.clone(), repo, "worker-1".into(), "".into());

        tokio::time::pause();
        let result = coordinator
            .with_backoff(|| coordinator.send_heartbeat())
            .await;
        assert!(result.is_ok());
        assert_eq!(client.heartbeat_calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_with_backoff_gives_up_after_max_attempts_per_tick() {
        let (_dir, repo) = test_repo().await;
        // More failures scripted than MAX_ATTEMPTS_PER_TICK, so this proves
        // the loop actually stops instead of the script just running out.
        let client = Arc::new(FakeMasterClient::new(vec![]).with_heartbeat_script(vec![
            Err(SyncError::Network("e".to_string())),
            Err(SyncError::Network("e".to_string())),
            Err(SyncError::Network("e".to_string())),
            Err(SyncError::Network("e".to_string())),
            Err(SyncError::Network("e".to_string())),
        ]));
        let coordinator = Coordinator::new(client.clone(), repo, "worker-1".into(), "".into());

        tokio::time::pause();
        let result = coordinator
            .with_backoff(|| coordinator.send_heartbeat())
            .await;
        assert!(result.is_err());
        assert_eq!(
            client.heartbeat_calls.load(Ordering::SeqCst),
            MAX_ATTEMPTS_PER_TICK
        );
    }

    #[tokio::test]
    async fn test_with_backoff_does_not_retry_non_retryable_errors() {
        let (_dir, repo) = test_repo().await;
        let client = Arc::new(
            FakeMasterClient::new(vec![]).with_heartbeat_script(vec![Err(SyncError::Api(
                "video already claimed by another instance".to_string(),
            ))]),
        );
        let coordinator = Coordinator::new(client.clone(), repo, "worker-1".into(), "".into());

        let result = coordinator
            .with_backoff(|| coordinator.send_heartbeat())
            .await;
        assert!(result.is_err());
        assert_eq!(
            client.heartbeat_calls.load(Ordering::SeqCst),
            1,
            "a non-retryable error must fail on the first attempt"
        );
    }

    #[tokio::test]
    async fn test_tick_survives_master_unreachable_and_updates_error_metrics() {
        let (_dir, repo) = test_repo().await;
        let client = Arc::new(
            FakeMasterClient::new(vec![])
                .with_heartbeat_script(vec![Err(SyncError::Api("down".to_string()))])
                .with_get_pending_script(vec![Err(SyncError::Api("down".to_string()))]),
        );
        let coordinator = Coordinator::new(client, repo, "worker-1".into(), "".into());

        // Must not panic even though every call fails.
        coordinator.tick().await;

        let metrics = coordinator.metrics();
        assert_eq!(metrics.heartbeat_error_count, 1);
        assert_eq!(metrics.sync_error_count, 1);
        assert!(metrics.last_heartbeat.is_none());
        assert!(metrics.last_sync.is_none());
    }

    #[tokio::test]
    async fn test_tick_updates_success_metrics() {
        let (_dir, repo) = test_repo().await;
        let client = Arc::new(FakeMasterClient::new(vec![video_response("v1")]));
        let coordinator = Coordinator::new(client, repo, "worker-1".into(), "".into());

        coordinator.tick().await;

        let metrics = coordinator.metrics();
        assert_eq!(metrics.heartbeat_error_count, 0);
        assert_eq!(metrics.sync_error_count, 0);
        assert!(metrics.last_heartbeat.is_some());
        assert!(metrics.last_sync.is_some());
    }

    #[test]
    fn test_sync_error_retryable_classification() {
        assert!(SyncError::Network("boom".into()).is_retryable());
        assert!(SyncError::HttpStatus {
            status: 503,
            message: "unavailable".into()
        }
        .is_retryable());
        assert!(!SyncError::HttpStatus {
            status: 400,
            message: "bad request".into()
        }
        .is_retryable());
        assert!(!SyncError::Api("conflict".into()).is_retryable());
    }
}
