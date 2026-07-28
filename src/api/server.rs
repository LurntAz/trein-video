use crate::api::handlers::{self, SharedRepository};
use crate::config::Config;
use crate::db::{DbConnection, Repository};
use crate::tls::{ensure_certificates, TlsError, TlsMaterial, TlsPaths};
use axum::routing::{get, post};
use axum::Router;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use rustls::server::WebPkiClientVerifier;
use rustls::RootCertStore;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::{Arc, Once};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tracing::{info, instrument, warn};

static INSTALL_CRYPTO_PROVIDER: Once = Once::new();

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("tls setup failed: {0}")]
    Tls(#[from] TlsError),
    #[error("failed to build rustls server config: {0}")]
    Rustls(#[from] rustls::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("no valid certificate found in PEM data")]
    NoCertificate,
    #[error("no valid private key found in PEM data")]
    NoPrivateKey,
    #[error("failed to build client certificate verifier: {0}")]
    ClientVerifier(String),
    #[error("database initialization failed: {0}")]
    Db(#[from] anyhow::Error),
}

/// Axum + manual mTLS server.
///
/// `axum-server` 0.6 (the latest release on crates.io) fails to compile
/// against the `hyper-util` version pulled in transitively by `axum` 0.7
/// (`E0277: BodyData: Buf` inside its HTTP/2-upgrade accept loop) — a known
/// version-skew issue with no fixed release available. We terminate TLS
/// ourselves instead: a plain `TcpListener` accept loop, `tokio_rustls` for
/// the handshake (with a client-certificate verifier for mTLS), and
/// `hyper::server::conn::http1` to drive the axum `Router` per connection.
/// This is the same low-level recipe axum's own examples used before
/// `axum-server` existed.
pub struct ApiServer {
    config: Config,
}

impl ApiServer {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Build the full application router: `/health` (#12) plus the
    /// videos/instances endpoints (#14), all sharing one `Repository`
    /// (backed by this instance's local SQLite DB) as axum `State`.
    fn router(repository: SharedRepository) -> Router {
        Router::new()
            .route("/health", get(handlers::health_handler))
            .route("/api/videos/pending", get(handlers::get_pending_videos))
            .route(
                "/api/videos/:id/status",
                get(handlers::get_video_status).post(handlers::update_video_status),
            )
            .route("/api/instances/heartbeat", post(handlers::heartbeat))
            .route("/api/instances", get(handlers::get_instances))
            .with_state(repository)
    }

    /// Set up TLS + DB + the TCP listener, but don't start accepting
    /// connections yet. Split out from [`ApiServer::start`] so tests (#16)
    /// can bind on an OS-assigned ephemeral port (`instance.api_port = 0`)
    /// and read back the real port via [`BoundApiServer::local_addr`] before
    /// handing off to [`BoundApiServer::serve`], which is the infinite
    /// accept loop.
    #[instrument(skip(self), fields(instance_id = %self.config.instance.id))]
    pub async fn bind(&self) -> Result<BoundApiServer, ServerError> {
        info!("bind() - starting TLS setup");
        let paths = TlsPaths {
            cert_path: self.config.tls.cert_path.clone().into(),
            key_path: self.config.tls.key_path.clone().into(),
            ca_cert_path: self.config.tls.ca_cert_path.clone().into(),
        };
        info!("bind() - ensuring certificates");
        let material = ensure_certificates(&paths).await?;
        info!("bind() - building TLS config");
        let tls_config = build_server_tls_config(&material)?;
        let acceptor = TlsAcceptor::from(Arc::new(tls_config));
        info!("bind() - TLS setup complete");

        info!(db_path = %self.config.db.path, "bind() - initializing database");
        let db_connection = DbConnection::new(&self.config.db.path).await?;
        info!("bind() - database connection established");
        let repository: SharedRepository = Arc::new(Repository::new(db_connection.pool().clone()));

        let addr: SocketAddr = format!("0.0.0.0:{}", self.config.instance.api_port)
            .parse()
            .expect("host:port string is always a valid SocketAddr");

        info!(%addr, "bind() - binding TCP listener");
        let listener = TcpListener::bind(addr).await.map_err(|e| {
            warn!(%addr, error = %e, "failed to bind API server port");
            ServerError::Io(e)
        })?;
        let local_addr = listener.local_addr().map_err(ServerError::Io)?;

        info!(%local_addr, "mTLS API server bound");

        let app = Self::router(repository);

        Ok(BoundApiServer {
            local_addr,
            listener,
            acceptor,
            app,
        })
    }

    /// Bind and serve forever. Equivalent to `self.bind().await?.serve().await`;
    /// this is what `main.rs` uses in production, where the port is known
    /// upfront from config and there's no need to inspect it before serving.
    pub async fn start(&self) -> Result<(), ServerError> {
        info!("ApiServer::start - binding");
        let bound = self.bind().await?;
        info!(addr = %bound.local_addr, "ApiServer::start - bound, now serving");
        bound.serve().await
    }
}

/// A [`TcpListener`] already bound and ready to accept mTLS connections, plus
/// the fully-built axum [`Router`]. See [`ApiServer::bind`] for why this is
/// split out from [`ApiServer::start`].
pub struct BoundApiServer {
    local_addr: SocketAddr,
    listener: TcpListener,
    acceptor: TlsAcceptor,
    app: Router,
}

impl BoundApiServer {
    /// The actual address/port this server is listening on -- in
    /// particular, the OS-assigned port when `instance.api_port` was `0`.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Accept connections forever, terminating mTLS and serving `app` over
    /// HTTP/1.1 on each one. Never returns `Ok`; only returns at all if the
    /// process should exit due to a fatal setup error (there currently is
    /// none after `bind()` has already succeeded, so in practice this runs
    /// until the process is killed).
    pub async fn serve(self) -> Result<(), ServerError> {
        let BoundApiServer {
            listener,
            acceptor,
            app,
            ..
        } = self;

        loop {
            let (stream, peer_addr) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    warn!(error = %e, "failed to accept TCP connection");
                    continue;
                }
            };
            let acceptor = acceptor.clone();
            let app = app.clone();

            tokio::spawn(async move {
                let tls_stream = match acceptor.accept(stream).await {
                    Ok(s) => s,
                    Err(e) => {
                        // Rejected here means: no client cert, or a cert not
                        // signed by our CA. Don't leak crypto internals,
                        // just note that the handshake failed.
                        warn!(%peer_addr, error = %e, "TLS handshake failed");
                        return;
                    }
                };
                let io = TokioIo::new(tls_stream);
                let service = TowerToHyperService::new(app);
                if let Err(e) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service)
                    .await
                {
                    warn!(%peer_addr, error = %e, "connection error");
                }
            });
        }
    }
}

fn install_crypto_provider() {
    INSTALL_CRYPTO_PROVIDER.call_once(|| {
        // rustls 0.23 requires a process-level default `CryptoProvider`
        // before `ServerConfig::builder()` can be used. Installing twice
        // (e.g. master + worker both start a server in the same process in
        // tests) would return an error we don't care about, hence `Once`.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn build_server_tls_config(material: &TlsMaterial) -> Result<rustls::ServerConfig, ServerError> {
    install_crypto_provider();

    let cert_chain = parse_certs(&material.cert_pem)?;
    let key = parse_private_key(&material.key_pem)?;
    let ca_certs = parse_certs(&material.ca_cert_pem)?;

    let mut root_store = RootCertStore::empty();
    for cert in ca_certs {
        root_store
            .add(cert)
            .map_err(|e| ServerError::ClientVerifier(e.to_string()))?;
    }

    // mTLS: require every connecting client to present a certificate signed
    // by our CA, not just the server presenting one to the client.
    let client_verifier = WebPkiClientVerifier::builder(Arc::new(root_store))
        .build()
        .map_err(|e| ServerError::ClientVerifier(e.to_string()))?;

    let config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(cert_chain, key)?;

    Ok(config)
}

fn parse_certs(pem: &str) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, ServerError> {
    let mut reader = Cursor::new(pem.as_bytes());
    let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<_, _>>()
        .map_err(ServerError::Io)?;
    if certs.is_empty() {
        return Err(ServerError::NoCertificate);
    }
    Ok(certs)
}

fn parse_private_key(pem: &str) -> Result<rustls::pki_types::PrivateKeyDer<'static>, ServerError> {
    let mut reader = Cursor::new(pem.as_bytes());
    rustls_pemfile::private_key(&mut reader)
        .map_err(ServerError::Io)?
        .ok_or(ServerError::NoPrivateKey)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tls::ensure_certificates;

    #[tokio::test]
    async fn test_build_server_tls_config_from_generated_certs() {
        let dir = tempfile::tempdir().unwrap();
        let paths = TlsPaths {
            cert_path: dir.path().join("server.crt"),
            key_path: dir.path().join("server.key"),
            ca_cert_path: dir.path().join("ca.crt"),
        };
        let material = ensure_certificates(&paths).await.unwrap();
        let config = build_server_tls_config(&material);
        assert!(config.is_ok(), "{:?}", config.err());
    }

    #[test]
    fn test_parse_certs_rejects_empty_pem() {
        let result = parse_certs("");
        assert!(matches!(result, Err(ServerError::NoCertificate)));
    }
}
