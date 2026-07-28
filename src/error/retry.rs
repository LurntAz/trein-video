//! Centralized retry policy (#17).
//!
//! Rather than each caller of a network/IO operation (#8's `SmbClient`, #9's
//! upload, #11's pipeline stages, #15's sync coordinator) re-implementing its
//! own backoff loop, this module provides:
//!
//! - [`Retryable`]: a trait each error type in the codebase can implement to
//!   classify itself as transient (worth retrying) or permanent (fail fast).
//! - [`retry_with_backoff`]: a generic retry loop for any `Result<T, E>`
//!   where `E: Retryable`.
//! - [`compute_backoff`]: exponential backoff with jitter, shared by every
//!   retry loop in the codebase so they all behave consistently.
//! - [`is_retryable_message`]: a pragmatic fallback classifier for the parts
//!   of the codebase (`worker::processor`'s `DownloadStage`/`UploadStage`
//!   traits, #11) that intentionally type-erase their errors to `String` so
//!   fakes are trivial to write in tests. It pattern-matches on well-known
//!   substrings rather than a concrete error type — see the table below.
//!
//! ## Error -> retryable classification
//!
//! | Error kind                                   | Retryable? |
//! |-----------------------------------------------|:----------:|
//! | Network/connection failure, timeout            | yes        |
//! | I/O error (transient disk/socket hiccup)       | yes        |
//! | SMB session expired mid-transfer               | yes        |
//! | DB "pool locked"/busy                          | yes        |
//! | Authentication failure                         | no         |
//! | Remote/local file not found                    | no         |
//! | Path conflict / already exists                 | no         |
//! | Size mismatch after transfer                   | no         |
//! | Invalid input / malformed request               | no         |
//!
//! Any future error type added to the codebase should implement
//! [`Retryable`] consistently with this table rather than guessing.

use crate::config::RetryConfig;
use std::future::Future;
use std::time::Duration;

/// Implemented by error types that can tell whether retrying the operation
/// that produced them is worthwhile.
pub trait Retryable {
    fn is_retryable(&self) -> bool;
}

impl Retryable for crate::nas::SmbError {
    fn is_retryable(&self) -> bool {
        use crate::nas::SmbError::*;
        matches!(self, ConnectionFailed(..) | SessionExpired(..) | Io(_))
    }
}

/// Exponential backoff (`base_delay_secs * 1.5^attempt`, capped at
/// `max_delay_secs`) with up to +/-20% jitter, so many workers retrying
/// against the same NAS/master at once don't all retry in lockstep.
///
/// `attempt` is 1-based (the first retry after an initial failure is
/// `attempt == 1`).
pub fn compute_backoff(attempt: u32, config: &RetryConfig) -> Duration {
    let base_ms = config.base_delay_secs.max(1) as f64 * 1000.0;
    let exp = 1.5f64.powi(attempt as i32);
    let uncapped_ms = base_ms * exp;
    let cap_ms = (config.max_delay_secs.max(1) as f64) * 1000.0;
    let capped_ms = uncapped_ms.min(cap_ms).max(1.0) as u64;

    let jitter_range = capped_ms / 5; // +/-20%
    if jitter_range == 0 {
        return Duration::from_millis(capped_ms);
    }
    // Cheap, non-cryptographic jitter source: nanosecond-resolution clock
    // reading. Good enough to spread out concurrent retries; this is not a
    // security-sensitive use of randomness.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0) as u64;
    let jitter = (nanos % (2 * jitter_range + 1)) as i64 - jitter_range as i64;
    let final_ms = (capped_ms as i64 + jitter).max(1) as u64;
    Duration::from_millis(final_ms)
}

/// Retry `op` according to `config`, stopping as soon as it succeeds, its
/// error reports itself as non-retryable, or `config.max_attempts` has been
/// reached. The last error is returned on exhaustion.
pub async fn retry_with_backoff<T, E, F, Fut>(config: &RetryConfig, mut op: F) -> Result<T, E>
where
    E: Retryable,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        match op().await {
            Ok(value) => return Ok(value),
            Err(e) if e.is_retryable() && attempt < config.max_attempts.max(1) => {
                tokio::time::sleep(compute_backoff(attempt, config)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Substrings that mark an error message (produced by a `Result<_, String>`
/// boundary, e.g. `worker::processor`'s stage traits) as worth retrying.
/// Checked only if no [`NON_RETRYABLE_MARKERS`] matched first, so a message
/// like "authentication failed: connection reset while renegotiating" is
/// still treated as permanent.
const RETRYABLE_MARKERS: [&str; 8] = [
    "timeout",
    "timed out",
    "connection",
    "session expired",
    "network",
    "temporarily unavailable",
    "pool is locked",
    "database is locked",
];

const NON_RETRYABLE_MARKERS: [&str; 8] = [
    "not found",
    "auth",
    "invalid",
    "conflict",
    "already exists",
    "permission denied",
    "no such file",
    "size mismatch",
];

/// Best-effort classification of a stringified error message as transient.
/// Unknown messages (matching neither list) are treated as non-retryable —
/// safer to fail fast on an unrecognized error than to loop indefinitely.
pub fn is_retryable_message(message: &str) -> bool {
    let lower = message.to_lowercase();
    if NON_RETRYABLE_MARKERS.iter().any(|m| lower.contains(m)) {
        return false;
    }
    RETRYABLE_MARKERS.iter().any(|m| lower.contains(m))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn fast_config() -> RetryConfig {
        // Zero delay so these tests don't actually wait on real backoff.
        RetryConfig {
            max_attempts: 5,
            base_delay_secs: 0,
            max_delay_secs: 0,
        }
    }

    #[derive(Debug)]
    struct MockTransientError;
    impl Retryable for MockTransientError {
        fn is_retryable(&self) -> bool {
            true
        }
    }

    #[derive(Debug)]
    struct MockPermanentError;
    impl Retryable for MockPermanentError {
        fn is_retryable(&self) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn test_retry_with_backoff_succeeds_after_transient_failures() {
        let attempts = AtomicU32::new(0);
        let result: Result<&str, MockTransientError> = retry_with_backoff(&fast_config(), || {
            let n = attempts.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                if n < 3 {
                    Err(MockTransientError)
                } else {
                    Ok("success")
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), "success");
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_with_backoff_exhausts_after_max_attempts() {
        let attempts = AtomicU32::new(0);
        let config = RetryConfig {
            max_attempts: 3,
            ..fast_config()
        };
        let result: Result<(), MockTransientError> = retry_with_backoff(&config, || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Err(MockTransientError) }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_with_backoff_permanent_error_fails_immediately() {
        let attempts = AtomicU32::new(0);
        let result: Result<(), MockPermanentError> = retry_with_backoff(&fast_config(), || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Err(MockPermanentError) }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "a non-retryable error must not be retried"
        );
    }

    #[test]
    fn test_compute_backoff_increases_and_respects_cap() {
        let config = RetryConfig {
            max_attempts: 10,
            base_delay_secs: 1,
            max_delay_secs: 5,
        };
        let b1 = compute_backoff(1, &config);
        let b5 = compute_backoff(5, &config);
        assert!(b5 >= b1, "backoff should grow with attempt count");
        assert!(
            b5 <= Duration::from_millis(5000 + 5000 / 5),
            "backoff must respect max_delay_secs (plus jitter): got {b5:?}"
        );
    }

    #[test]
    fn test_smb_error_retryable_classification() {
        use crate::nas::SmbError;
        assert!(SmbError::ConnectionFailed("h".into(), "s".into(), "e".into()).is_retryable());
        assert!(SmbError::SessionExpired("e".into()).is_retryable());
        assert!(SmbError::Io(std::io::Error::other("boom")).is_retryable());
        assert!(!SmbError::AuthFailed("user".into()).is_retryable());
        assert!(!SmbError::NotFound("f".into()).is_retryable());
        assert!(!SmbError::BinaryNotFound.is_retryable());
    }

    #[test]
    fn test_is_retryable_message_recognizes_transient_errors() {
        assert!(is_retryable_message("download failed: connection timeout"));
        assert!(is_retryable_message(
            "smb session expired or connection lost"
        ));
        assert!(is_retryable_message("database is locked"));
    }

    #[test]
    fn test_is_retryable_message_recognizes_permanent_errors() {
        assert!(!is_retryable_message(
            "authentication failed for user 'bob'"
        ));
        assert!(!is_retryable_message("remote path not found: foo.mp4"));
        assert!(!is_retryable_message(
            "downloaded file size mismatch: expected 10 bytes, got 5"
        ));
    }
}
