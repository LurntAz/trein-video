//! End-to-end tests for the master API server (#12/#14) together with the
//! worker sync coordinator (#15): a real `ApiServer` (real mTLS handshake,
//! real self-signed certs generated on the fly via `rcgen`, real SQLite DB)
//! bound on an OS-assigned ephemeral port, talked to by a real
//! `HttpMasterClient`/`Coordinator` -- no mocked HTTP layer.
//!
//! Every test gets its own `tempfile::tempdir()` (certs + master DB +
//! worker DB all live under it) so tests can run in parallel (`cargo test`'s
//! default) without interfering with each other, per #16's plan.

use std::sync::Arc;
use std::time::Duration;
use trein_video::api::server::ApiServer;
use trein_video::config::{
    Config, ConversionConfig, DbConfig, DiscoveryConfig, InstanceConfig, NasConfig, RetryConfig,
    SyncConfig, TlsConfig,
};
use trein_video::db::{DbConnection, Repository, Video};
use trein_video::sync::{Coordinator, HttpMasterClient, SyncError};

fn path_str(p: &std::path::Path) -> String {
    p.to_string_lossy().to_string()
}

/// A master `Config` bound to an ephemeral port (`api_port: 0`), with fresh
/// certs generated under `dir` on first use.
fn master_config(dir: &std::path::Path) -> Config {
    Config {
        instance: InstanceConfig {
            id: "master-1".to_string(),
            role: "master".to_string(),
            api_port: 0,
        },
        nas: NasConfig {
            protocol: "smb".to_string(),
            host: "127.0.0.1".to_string(),
            share: "videos".to_string(),
            username: "user".to_string(),
            password: None,
            base_path: "/videos".to_string(),
        },
        conversion: ConversionConfig {
            codec: "av1".to_string(),
            preset: "slow".to_string(),
            crf: 32,
            max_parallel_jobs: 1,
        },
        sync: SyncConfig {
            poll_interval_secs: 1,
            master_url: None,
        },
        db: DbConfig {
            path: path_str(&dir.join("master.db")),
        },
        tls: TlsConfig {
            cert_path: path_str(&dir.join("certs/server.crt")),
            key_path: path_str(&dir.join("certs/server.key")),
            ca_cert_path: path_str(&dir.join("certs/ca.crt")),
        },
        discovery: DiscoveryConfig {
            enabled: false,
            service_name: "test".to_string(),
        },
        retry: RetryConfig::default(),
    }
}

/// TLS material for a worker's `HttpMasterClient`: the client cert/key
/// `ApiServer::bind` (via `tls::ensure_certificates`, #12) writes alongside
/// the CA the first time it generates certs, trusting the same CA.
fn worker_tls(dir: &std::path::Path) -> TlsConfig {
    TlsConfig {
        cert_path: path_str(&dir.join("certs/client.crt")),
        key_path: path_str(&dir.join("certs/client.key")),
        ca_cert_path: path_str(&dir.join("certs/ca.crt")),
    }
}

/// Start a real master `ApiServer` on an ephemeral port and return its
/// address. The server keeps running (spawned) for the test's duration.
async fn start_master(dir: &std::path::Path) -> String {
    let server = ApiServer::new(master_config(dir));
    let bound = server.bind().await.expect("failed to bind master server");
    let addr = bound.local_addr();
    tokio::spawn(bound.serve());
    format!("https://127.0.0.1:{}", addr.port())
}

/// Build a `Coordinator` acting as one worker, with its own local DB
/// (`worker_db_name`) distinct from the master's.
async fn worker_coordinator(
    dir: &std::path::Path,
    worker_db_name: &str,
    instance_id: &str,
    master_url: &str,
) -> (Arc<Repository>, Arc<Coordinator>) {
    let conn = DbConnection::new(dir.join(worker_db_name))
        .await
        .expect("failed to open worker DB");
    let repo = Arc::new(Repository::new(conn.pool().clone()));
    let client = HttpMasterClient::new(master_url.to_string(), &worker_tls(dir))
        .expect("failed to build mTLS client");
    let coordinator = Arc::new(Coordinator::new(
        Arc::new(client),
        repo.clone(),
        instance_id.to_string(),
        format!("https://{instance_id}:0"),
    ));
    (repo, coordinator)
}

fn pending_video(id: &str) -> Video {
    let now = chrono::Utc::now();
    Video {
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
        claimed_at: None,
        attempts: 0,
        last_retry_time: None,
    }
}

#[tokio::test]
async fn test_heartbeat_registers_worker_instance_on_master() {
    let dir = tempfile::tempdir().unwrap();
    let master_url = start_master(dir.path()).await;

    let (_worker_repo, coordinator) =
        worker_coordinator(dir.path(), "worker.db", "worker-1", &master_url).await;

    coordinator
        .send_heartbeat()
        .await
        .expect("heartbeat should succeed against a real running master");

    // Open a second connection to the master's own DB file to verify the
    // heartbeat was actually persisted -- WAL mode (#4) allows this
    // alongside the server's own connection.
    let master_conn = DbConnection::new(dir.path().join("master.db"))
        .await
        .unwrap();
    let master_repo = Repository::new(master_conn.pool().clone());
    let instances = master_repo.get_instances().await.unwrap();
    assert_eq!(instances.len(), 1);
    assert_eq!(instances[0].id, "worker-1");
    assert_eq!(instances[0].role, "worker");
}

#[tokio::test]
async fn test_submit_video_syncs_to_worker_and_status_updates_flow_back_to_master() {
    let dir = tempfile::tempdir().unwrap();
    let master_url = start_master(dir.path()).await;

    // "Submit a video": inserted directly into the master's DB (in the real
    // system this would come from whatever scans the NAS/enqueues work --
    // out of scope here, #16 only concerns the master<->worker leg).
    let master_conn = DbConnection::new(dir.path().join("master.db"))
        .await
        .unwrap();
    let master_repo = Repository::new(master_conn.pool().clone());
    master_repo
        .insert_video(&pending_video("v1"))
        .await
        .unwrap();

    let (worker_repo, coordinator) =
        worker_coordinator(dir.path(), "worker.db", "worker-1", &master_url).await;

    // Assigned to the worker: sync pulls it down into the worker's own local
    // queue (#10 claims from here, independent of the master).
    let inserted = coordinator.sync_with_master().await.unwrap();
    assert_eq!(inserted, 1);
    assert!(worker_repo.get_video("v1").await.unwrap().is_some());

    // Processing: the worker claims it locally and reports progress back.
    let claimed = worker_repo
        .claim_next_pending("worker-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id, "v1");
    coordinator
        .report_status("v1", "downloading", None, None)
        .await
        .unwrap();
    coordinator
        .report_status("v1", "converting", None, None)
        .await
        .unwrap();
    coordinator
        .report_status("v1", "uploading", None, None)
        .await
        .unwrap();

    // Done: final status + size relayed back.
    coordinator
        .report_status("v1", "done", None, Some(123_456))
        .await
        .unwrap();

    let master_video = master_repo.get_video("v1").await.unwrap().unwrap();
    assert_eq!(master_video.status, "done");
    assert_eq!(master_video.converted_size_bytes, Some(123_456));
    assert_eq!(master_video.instance_id.as_deref(), Some("worker-1"));
}

#[tokio::test]
async fn test_second_worker_does_not_receive_video_already_claimed_by_first() {
    // Demonstrates the mechanism #15 relies on to avoid double-processing
    // across independent workers (each with their own local DB, unlike the
    // single-DB atomic `claim_next_pending` used *within* one worker):
    // once worker A's coordinator reports a video as no longer `pending`,
    // the master's `GET /api/videos/pending` (#14) stops listing it, so a
    // worker B that syncs afterwards never sees it.
    let dir = tempfile::tempdir().unwrap();
    let master_url = start_master(dir.path()).await;

    let master_conn = DbConnection::new(dir.path().join("master.db"))
        .await
        .unwrap();
    let master_repo = Repository::new(master_conn.pool().clone());
    master_repo
        .insert_video(&pending_video("v1"))
        .await
        .unwrap();

    let (worker_a_repo, coordinator_a) =
        worker_coordinator(dir.path(), "worker_a.db", "worker-a", &master_url).await;
    let (worker_b_repo, coordinator_b) =
        worker_coordinator(dir.path(), "worker_b.db", "worker-b", &master_url).await;

    // Worker A syncs first, claims it, and reports the transition.
    coordinator_a.sync_with_master().await.unwrap();
    worker_a_repo.claim_next_pending("worker-a").await.unwrap();
    coordinator_a
        .report_status("v1", "downloading", None, None)
        .await
        .unwrap();

    // Worker B syncs afterwards: the master no longer lists v1 as pending.
    let inserted_for_b = coordinator_b.sync_with_master().await.unwrap();
    assert_eq!(inserted_for_b, 0);
    assert!(
        worker_b_repo.get_video("v1").await.unwrap().is_none(),
        "worker B must not receive a video already claimed by worker A"
    );
}

#[tokio::test]
async fn test_report_status_from_wrong_instance_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let master_url = start_master(dir.path()).await;

    let master_conn = DbConnection::new(dir.path().join("master.db"))
        .await
        .unwrap();
    let master_repo = Repository::new(master_conn.pool().clone());
    master_repo
        .insert_video(&pending_video("v1"))
        .await
        .unwrap();

    let (worker_a_repo, coordinator_a) =
        worker_coordinator(dir.path(), "worker_a.db", "worker-a", &master_url).await;
    let (_worker_b_repo, coordinator_b) =
        worker_coordinator(dir.path(), "worker_b.db", "worker-b", &master_url).await;

    coordinator_a.sync_with_master().await.unwrap();
    worker_a_repo.claim_next_pending("worker-a").await.unwrap();
    coordinator_a
        .report_status("v1", "downloading", None, None)
        .await
        .unwrap();

    // Worker B never claimed v1 but tries to report on it anyway (e.g. a
    // stale/buggy report) -- the master (#14's `is_valid_transition`
    // ownership check) must reject it with a non-retryable error rather
    // than letting it clobber worker A's job.
    let result = coordinator_b
        .report_status("v1", "failed", Some("wrong worker".to_string()), None)
        .await;
    assert!(result.is_err());
    match result.unwrap_err() {
        SyncError::HttpStatus { status, .. } => assert_eq!(status, 409),
        other => panic!("expected an HTTP 409 conflict, got {other:?}"),
    }

    // Confirm it wasn't actually mutated.
    let video = master_repo.get_video("v1").await.unwrap().unwrap();
    assert_eq!(video.status, "downloading");
}

#[tokio::test]
async fn test_coordinator_survives_master_being_unreachable() {
    // Network partition: point a coordinator at a master URL nothing is
    // listening on. `tick()` must not panic, and must be reflected purely
    // as error metrics -- see `Coordinator::tick`'s docs on why this must
    // degrade gracefully rather than taking the worker down.
    let dir = tempfile::tempdir().unwrap();

    // Generate real certs (needed for `HttpMasterClient::new` to succeed)
    // without actually starting a server to serve them.
    let _ = ApiServer::new(master_config(dir.path()))
        .bind()
        .await
        .expect("cert generation via bind() should succeed even though we never serve");

    // Port 1 on loopback: reserved, nothing will ever accept there.
    let (_worker_repo, coordinator) =
        worker_coordinator(dir.path(), "worker.db", "worker-1", "https://127.0.0.1:1").await;

    let result = tokio::time::timeout(Duration::from_secs(20), async {
        coordinator.send_heartbeat().await
    })
    .await
    .expect("must not hang indefinitely against an unreachable master");
    assert!(result.is_err());

    coordinator.tick().await;
    let metrics = coordinator.metrics();
    assert!(metrics.heartbeat_error_count > 0);
    assert!(metrics.sync_error_count > 0);
    assert!(metrics.last_heartbeat.is_none());
    assert!(metrics.last_sync.is_none());
}
