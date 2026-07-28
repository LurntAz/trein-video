//! Self-signed CA/server/client certificate bootstrap for mTLS.
//!
//! `trein-video` instances are expected to run on a single trusted home/LAN
//! network for one user, so we don't integrate with an external PKI: on
//! first boot the master generates a CA plus a server and client
//! certificate signed by it, and writes them to the paths configured under
//! `[tls]`. Workers must be given a copy of the same `ca_cert_path` (and a
//! client cert/key signed by that CA) out of band — see the README. If the
//! configured files already exist, they are loaded as-is instead of being
//! regenerated.

use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair, KeyUsagePurpose};
use std::path::PathBuf;
use thiserror::Error;
use time::{Duration as TimeDuration, OffsetDateTime};
use tracing::info;

/// Self-signed certs are valid for 10 years — appropriate for a solo/home
/// deployment where automated rotation is out of scope; see module docs.
const CERT_VALIDITY_DAYS: i64 = 365 * 10;

#[derive(Debug, Error)]
pub enum TlsError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("certificate generation failed: {0}")]
    Generation(#[from] rcgen::Error),
}

/// Paths to the three PEM files that make up our mTLS trust setup.
#[derive(Debug, Clone)]
pub struct TlsPaths {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub ca_cert_path: PathBuf,
}

/// A loaded (or freshly generated) server identity + CA trust anchor, ready
/// to be turned into a `rustls::ServerConfig`.
pub struct TlsMaterial {
    pub cert_pem: String,
    pub key_pem: String,
    pub ca_cert_pem: String,
}

/// Ensure the certificates described by `paths` exist, generating a fresh
/// self-signed CA + server certificate if any of the three files is
/// missing, then return their PEM contents.
pub async fn ensure_certificates(paths: &TlsPaths) -> Result<TlsMaterial, TlsError> {
    let all_present =
        paths.cert_path.exists() && paths.key_path.exists() && paths.ca_cert_path.exists();

    if !all_present {
        info!(
            cert = %paths.cert_path.display(),
            "TLS certificates missing, generating a self-signed CA + server certificate"
        );
        generate_and_write(paths).await?;
    }

    let cert_pem = tokio::fs::read_to_string(&paths.cert_path).await?;
    let key_pem = tokio::fs::read_to_string(&paths.key_path).await?;
    let ca_cert_pem = tokio::fs::read_to_string(&paths.ca_cert_path).await?;

    Ok(TlsMaterial {
        cert_pem,
        key_pem,
        ca_cert_pem,
    })
}

async fn generate_and_write(paths: &TlsPaths) -> Result<(), TlsError> {
    let generated = generate_ca_and_server_cert()?;

    for path in [&paths.cert_path, &paths.key_path, &paths.ca_cert_path] {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
    }

    tokio::fs::write(&paths.ca_cert_path, &generated.ca_cert_pem).await?;
    tokio::fs::write(&paths.cert_path, &generated.server_cert_pem).await?;
    tokio::fs::write(&paths.key_path, &generated.server_key_pem).await?;

    // The client cert/key aren't referenced by `TlsConfig` (workers get
    // their own copy out of band, see module docs), but we write them next
    // to the CA so a fresh master install has something to hand to the
    // first worker without needing a separate tool.
    if let Some(parent) = paths.ca_cert_path.parent() {
        tokio::fs::write(parent.join("client.crt"), &generated.client_cert_pem).await?;
        tokio::fs::write(parent.join("client.key"), &generated.client_key_pem).await?;
    }

    Ok(())
}

struct GeneratedMaterial {
    ca_cert_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    client_cert_pem: String,
    client_key_pem: String,
}

fn generate_ca_and_server_cert() -> Result<GeneratedMaterial, rcgen::Error> {
    let not_before = OffsetDateTime::now_utc();
    let not_after = not_before + TimeDuration::days(CERT_VALIDITY_DAYS);

    // --- CA ---
    let mut ca_params = CertificateParams::new(Vec::new())?;
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    ca_params.not_before = not_before;
    ca_params.not_after = not_after;
    let mut ca_dn = DistinguishedName::new();
    ca_dn.push(DnType::CommonName, "trein-video local CA");
    ca_params.distinguished_name = ca_dn;

    let ca_key = KeyPair::generate()?;
    let ca_cert = ca_params.self_signed(&ca_key)?;

    // --- Server cert (used for both master and worker `/health` listeners) ---
    let server_cert_pair = leaf_cert(
        "trein-video-server",
        vec!["localhost".to_string()],
        not_before,
        not_after,
        &ca_cert,
        &ca_key,
    )?;

    // --- Client cert (presented by callers for mTLS) ---
    let client_cert_pair = leaf_cert(
        "trein-video-client",
        vec![],
        not_before,
        not_after,
        &ca_cert,
        &ca_key,
    )?;

    Ok(GeneratedMaterial {
        ca_cert_pem: ca_cert.pem(),
        server_cert_pem: server_cert_pair.0,
        server_key_pem: server_cert_pair.1,
        client_cert_pem: client_cert_pair.0,
        client_key_pem: client_cert_pair.1,
    })
}

fn leaf_cert(
    common_name: &str,
    subject_alt_names: Vec<String>,
    not_before: OffsetDateTime,
    not_after: OffsetDateTime,
    ca_cert: &rcgen::Certificate,
    ca_key: &KeyPair,
) -> Result<(String, String), rcgen::Error> {
    let mut params = CertificateParams::new(subject_alt_names)?;
    params.not_before = not_before;
    params.not_after = not_after;
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, common_name);
    params.distinguished_name = dn;

    let key = KeyPair::generate()?;
    let cert = params.signed_by(&key, ca_cert, ca_key)?;
    Ok((cert.pem(), key.serialize_pem()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ensure_certificates_generates_when_missing() {
        let dir = tempfile::tempdir().unwrap();
        let paths = TlsPaths {
            cert_path: dir.path().join("certs/server.crt"),
            key_path: dir.path().join("certs/server.key"),
            ca_cert_path: dir.path().join("certs/ca.crt"),
        };

        let material = ensure_certificates(&paths).await.unwrap();
        assert!(material.cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(material.key_pem.contains("PRIVATE KEY"));
        assert!(material.ca_cert_pem.contains("BEGIN CERTIFICATE"));

        assert!(paths.cert_path.exists());
        assert!(paths.key_path.exists());
        assert!(paths.ca_cert_path.exists());
        assert!(dir.path().join("certs/client.crt").exists());
        assert!(dir.path().join("certs/client.key").exists());
    }

    #[tokio::test]
    async fn test_ensure_certificates_reuses_existing_files() {
        let dir = tempfile::tempdir().unwrap();
        let paths = TlsPaths {
            cert_path: dir.path().join("certs/server.crt"),
            key_path: dir.path().join("certs/server.key"),
            ca_cert_path: dir.path().join("certs/ca.crt"),
        };

        let first = ensure_certificates(&paths).await.unwrap();
        let second = ensure_certificates(&paths).await.unwrap();

        assert_eq!(first.cert_pem, second.cert_pem);
        assert_eq!(first.ca_cert_pem, second.ca_cert_pem);
    }
}
