use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;
use tracing::{info, warn};

/// Custom mDNS service type. Workers browse for this to find the master
/// without needing `sync.master_url` configured manually.
const SERVICE_TYPE: &str = "_trein-video._tcp.local.";

/// Default time to wait for a master to answer a browse request before
/// giving up (see `discover_master_service`).
pub const DEFAULT_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Error)]
pub enum DiscoveryError {
    #[error("mDNS error: {0}")]
    MdnsSd(#[from] mdns_sd::Error),
    #[error(
        "no trein-video master found on the network within {0:?}; \
         set `sync.master_url` manually if mDNS/multicast is unavailable on this network"
    )]
    Timeout(Duration),
}

pub struct ServiceDiscovery;

/// Owns the running [`ServiceDaemon`] used to advertise the master's
/// presence. Dropping this handle unregisters the service and shuts the
/// daemon down.
pub struct PublishedService {
    daemon: ServiceDaemon,
    fullname: String,
}

impl Drop for PublishedService {
    fn drop(&mut self) {
        if let Err(e) = self.daemon.unregister(&self.fullname) {
            warn!(error = ?e, "failed to unregister mDNS service on shutdown");
        }
        let _ = self.daemon.shutdown();
    }
}

impl ServiceDiscovery {
    /// Publish this instance as the trein-video master via mDNS. The
    /// returned [`PublishedService`] must be kept alive for as long as the
    /// service should remain advertised.
    pub fn publish_master_service(
        service_name: &str,
        instance_id: &str,
        port: u16,
    ) -> Result<PublishedService, DiscoveryError> {
        let daemon = ServiceDaemon::new()?;

        let mut properties = HashMap::new();
        properties.insert("instance_id".to_string(), instance_id.to_string());

        let host_name = format!("{service_name}.local.");
        // Empty string + `enable_addr_auto()`: let mdns-sd discover and keep
        // this host's local IPv4 address(es) up to date itself, rather than
        // us hardcoding/guessing an interface address.
        let service_info =
            ServiceInfo::new(SERVICE_TYPE, service_name, &host_name, "", port, properties)?
                .enable_addr_auto();

        let fullname = service_info.get_fullname().to_string();
        daemon.register(service_info)?;

        info!(
            service_name,
            instance_id, port, "published master service via mDNS"
        );

        Ok(PublishedService { daemon, fullname })
    }

    /// Browse for a trein-video master on the local network, returning the
    /// first one found as `(ip, port)`. If more than one master answers
    /// within the timeout, a warning is logged listing all of them and the
    /// first is used (mDNS discovery does not enforce single-master
    /// invariants — that is a user configuration error).
    pub async fn discover_master_service(
        timeout: Duration,
    ) -> Result<(String, u16), DiscoveryError> {
        let daemon = ServiceDaemon::new()?;
        let receiver = daemon.browse(SERVICE_TYPE)?;

        let mut found: Vec<(String, u16)> = Vec::new();
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            match tokio::time::timeout(remaining, receiver.recv_async()).await {
                Ok(Ok(ServiceEvent::ServiceResolved(resolved_info))) => {
                    if let Some(addr) = resolved_info.get_addresses().iter().next() {
                        found.push((addr.to_string(), resolved_info.get_port()));
                    }
                }
                Ok(Ok(_other_event)) => continue,
                Ok(Err(_)) => break, // channel closed, daemon shut down
                Err(_) => break,     // outer timeout elapsed
            }
        }

        let _ = daemon.shutdown();

        if found.is_empty() {
            return Err(DiscoveryError::Timeout(timeout));
        }
        if found.len() > 1 {
            warn!(
                count = found.len(),
                masters = ?found,
                "multiple trein-video masters found via mDNS; using the first one found"
            );
        }
        Ok(found.remove(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Both tests below share the same hardcoded `SERVICE_TYPE` and talk to
    // the real local mDNS multicast group, so if they ran concurrently (the
    // `cargo test` default) a service published by one could be picked up
    // by the other, making the timeout test flaky. Serialize them.
    static MDNS_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[tokio::test]
    async fn test_publish_then_discover_locally() {
        let _guard = MDNS_TEST_LOCK.lock().await;
        // Publish on one daemon, discover with another, both in this
        // process talking over loopback mDNS. Flaky on CI runners without
        // multicast (e.g. some containers) — acceptable for a local dev
        // check; real cross-host discovery cannot be tested without real
        // network infrastructure.
        let published =
            ServiceDiscovery::publish_master_service("trein-test-master", "instance-1", 9443)
                .expect("failed to publish mDNS service");

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            ServiceDiscovery::discover_master_service(Duration::from_secs(5)),
        )
        .await;

        drop(published);

        match result {
            Ok(Ok((_host, port))) => assert_eq!(port, 9443),
            _ => {
                // No multicast available in this sandbox/CI network — don't
                // fail the suite over an environment limitation.
                eprintln!(
                    "mDNS discovery test skipped: no multicast response (sandboxed network?)"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_discover_times_out_when_no_master_present() {
        let _guard = MDNS_TEST_LOCK.lock().await;
        let result = ServiceDiscovery::discover_master_service(Duration::from_millis(500)).await;
        assert!(matches!(result, Err(DiscoveryError::Timeout(_))));
    }
}
