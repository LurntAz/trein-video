use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tracing::{debug, info, instrument};

/// How often [`SmbClient::download_file_with_progress`] polls the
/// partially-downloaded `.part` file's size while `smbclient` runs, to
/// report [`ProgressCallback`] ticks -- there is no way to get incremental
/// progress out of smbclient's own non-interactive `-c` mode directly, so
/// this polls the filesystem effect of the transfer instead.
const PROGRESS_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Callback invoked with `(bytes_transferred, bytes_total)` each time
/// [`SmbClient::download_file_with_progress`] polls transfer progress.
/// Deliberately expressed in raw bytes rather than in terms of
/// `crate::progress::ProgressEvent` -- this module shouldn't need to know
/// about the progress-display subsystem; that translation (computing speed/
/// ETA and emitting `TransferProgress`) is [`crate::worker::downloader`]'s
/// job.
pub type ProgressCallback = Box<dyn FnMut(u64, u64) + Send>;

/// Errors from shelling out to `smbclient`. Kept distinct per the phase 3
/// plan so the retry logic added later (#17) can tell transient network
/// problems apart from permanent ones (bad credentials, missing file, ...).
#[derive(Debug, Error)]
pub enum SmbError {
    #[error("smbclient binary not found in PATH")]
    BinaryNotFound,
    #[error("connection to //{0}/{1} failed: {2}")]
    ConnectionFailed(String, String, String),
    #[error("authentication failed for user '{0}'")]
    AuthFailed(String),
    #[error("remote path not found: {0}")]
    NotFound(String),
    #[error("remote path already exists: {0}")]
    RemotePathConflict(String),
    #[error("smb session expired or connection lost mid-transfer")]
    SessionExpired(String),
    #[error("insufficient local disk space at {path}: need {needed} bytes, {available} available")]
    InsufficientDiskSpace {
        path: String,
        needed: u64,
        available: u64,
    },
    #[error("downloaded/uploaded file size mismatch: expected {expected} bytes, got {actual}")]
    SizeMismatch { expected: u64, actual: u64 },
    #[error("smbclient command failed: {0}")]
    CommandFailed(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct SmbClient {
    pub host: String,
    pub share: String,
    pub username: String,
    pub password: Option<String>,
}

impl SmbClient {
    pub fn new(host: String, share: String, username: String, password: Option<String>) -> Self {
        Self {
            host,
            share,
            username,
            password,
        }
    }

    fn service_url(&self) -> String {
        format!("//{}/{}", self.host, self.share)
    }

    fn userpass_arg(&self) -> String {
        match &self.password {
            Some(p) => format!("{}%{}", self.username, p),
            None => self.username.clone(),
        }
    }

    /// Run a single non-interactive `smbclient -c "<command>"` invocation
    /// and return its (sanitized) combined stdout+stderr on success.
    ///
    /// `command` is built by the caller using [`quote_smb_arg`] for any
    /// path components so that filenames with spaces/UTF-8 are passed
    /// through correctly instead of being concatenated into raw shell text.
    async fn run_command(&self, command: &str) -> Result<String, SmbError> {
        self.run_command_with_progress(command, None, None).await
    }

    /// Like [`Self::run_command`], additionally polling `poll`'s path for
    /// its current size every [`PROGRESS_POLL_INTERVAL`] while the
    /// `smbclient` child process runs, invoking `on_progress` with
    /// `(current_size, poll.1)` on each tick (#20). `poll: None` (or
    /// `on_progress: None`) behaves exactly like [`Self::run_command`], just
    /// spawning the child directly instead of via `Command::output()`.
    async fn run_command_with_progress(
        &self,
        command: &str,
        poll: Option<(&Path, u64)>,
        mut on_progress: Option<ProgressCallback>,
    ) -> Result<String, SmbError> {
        let service = self.service_url();
        let userpass = self.userpass_arg();

        debug!(service = %service, "running smbclient command");

        let mut cmd = Command::new("smbclient");
        cmd.arg(&service)
            .args(["-U", &userpass])
            .args(["-c", command])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = cmd.spawn().map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                SmbError::BinaryNotFound
            } else {
                SmbError::Io(e)
            }
        })?;

        let mut child_stdout = child.stdout.take().expect("stdout was configured as piped");
        let mut child_stderr = child.stderr.take().expect("stderr was configured as piped");

        let status = if let Some((poll_path, total)) = poll {
            let poll_path = poll_path.to_path_buf();
            let mut interval = tokio::time::interval(PROGRESS_POLL_INTERVAL);
            interval.tick().await; // first tick fires immediately; skip it
            loop {
                tokio::select! {
                    result = child.wait() => break result?,
                    _ = interval.tick() => {
                        if let Some(cb) = on_progress.as_mut() {
                            let transferred = tokio::fs::metadata(&poll_path)
                                .await
                                .map(|m| m.len())
                                .unwrap_or(0);
                            cb(transferred, total);
                        }
                    }
                }
            }
        } else {
            child.wait().await?
        };

        // The process has already exited at this point, so these reads run
        // to EOF immediately; no risk of the classic "pipe buffer fills up
        // while nobody's reading it" deadlock that concurrent draining
        // (as in `video::converter`, for ffmpeg's much chattier stderr)
        // guards against.
        let mut stdout_buf = String::new();
        let mut stderr_buf = String::new();
        let _ = child_stdout.read_to_string(&mut stdout_buf).await;
        let _ = child_stderr.read_to_string(&mut stderr_buf).await;

        let combined = format!("{stdout_buf}\n{stderr_buf}");
        // Never let the NAS password leak into logs/error messages, even if
        // smbclient happens to echo the invoked command on failure.
        let sanitized = sanitize_smb_output(&combined, &self.username, self.password.as_deref());

        if !status.success() || contains_smb_error(&sanitized) {
            return Err(classify_smb_error(
                &sanitized,
                &self.host,
                &self.share,
                &self.username,
            ));
        }

        Ok(sanitized)
    }

    /// Health-check the connection/credentials without transferring anything.
    #[instrument(skip(self), fields(host = %self.host, share = %self.share))]
    pub async fn connect(&self) -> Result<(), SmbError> {
        self.run_command("quit").await?;
        Ok(())
    }

    /// List entries directly under `path` on the share.
    #[instrument(skip(self), fields(host = %self.host, share = %self.share))]
    pub async fn list_videos(&self, path: &str) -> Result<Vec<String>, SmbError> {
        let cmd = format!("ls {}", quote_smb_arg(&join_remote(path, "*")));
        let output = self.run_command(&cmd).await?;
        Ok(parse_ls_output(&output))
    }

    /// Download `remote_path` (relative to the share root) to `local_path`.
    ///
    /// Downloads to a `<local_path>.part` sibling file first and only
    /// renames it into place after the transfer completes successfully and
    /// its size matches what the NAS reports, so a job resumed after a
    /// crash never starts from a silently-truncated file.
    #[instrument(skip(self), fields(host = %self.host, share = %self.share, remote = %remote_path))]
    pub async fn download_file(
        &self,
        remote_path: &str,
        local_path: &Path,
    ) -> Result<(), SmbError> {
        self.download_file_with_progress(remote_path, local_path, None)
            .await
    }

    /// Like [`Self::download_file`], additionally reporting live byte-level
    /// progress via `on_progress` (#20), polled from the partially-
    /// downloaded `.part` file's growing size on disk against the remote
    /// file's known total size. `on_progress` is simply never called if the
    /// remote size couldn't be determined up front (same best-effort
    /// fallback the pre-existing disk-space check already relies on).
    #[instrument(skip(self, on_progress), fields(host = %self.host, share = %self.share, remote = %remote_path))]
    pub async fn download_file_with_progress(
        &self,
        remote_path: &str,
        local_path: &Path,
        on_progress: Option<ProgressCallback>,
    ) -> Result<(), SmbError> {
        if let Some(parent) = local_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let part_path = part_path_for(local_path);
        // Clean up a stale .part from a previous crashed attempt.
        let _ = tokio::fs::remove_file(&part_path).await;

        let (remote_dir, remote_file) = split_remote_path(remote_path);

        // Best-effort disk space check before transferring what could be a
        // multi-GB file: fail fast with a clear error rather than partway
        // through a download. Both `remote_file_size` and
        // `available_disk_space` are best-effort (e.g. `df` might not be on
        // PATH, or the remote stat might fail transiently) — we only refuse
        // to proceed when we positively know there isn't enough room,
        // never when the check itself is inconclusive. The same
        // `remote_size` also doubles as the total for progress reporting.
        let mut known_remote_size: Option<u64> = None;
        if let Ok(remote_size) = self.remote_file_size(&remote_dir, &remote_file).await {
            known_remote_size = Some(remote_size);
            let target_dir = local_path.parent().unwrap_or_else(|| Path::new("."));
            if let Ok(available) = available_disk_space(target_dir).await {
                if remote_size > available {
                    return Err(SmbError::InsufficientDiskSpace {
                        path: target_dir.display().to_string(),
                        needed: remote_size,
                        available,
                    });
                }
            }
        }

        let part_file_name = part_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| SmbError::CommandFailed("local path has no file name".to_string()))?;

        let cmd = format!(
            "lcd {}; cd {}; get {} {}",
            quote_smb_arg(&local_dir_str(&part_path)?),
            quote_smb_arg(&remote_dir),
            quote_smb_arg(&remote_file),
            quote_smb_arg(part_file_name),
        );

        let poll = known_remote_size.map(|size| (part_path.clone(), size));
        self.run_command_with_progress(
            &cmd,
            poll.as_ref().map(|(p, s)| (p.as_path(), *s)),
            on_progress,
        )
        .await?;

        if !part_path.exists() {
            return Err(SmbError::NotFound(remote_path.to_string()));
        }

        tokio::fs::rename(&part_path, local_path).await?;
        info!(local = %local_path.display(), "download complete");
        Ok(())
    }

    /// Upload `local_path` to `remote_path` (relative to the share root).
    ///
    /// Uploads to a `<remote_path>.uploading` sibling on the NAS first, then
    /// renames it into place server-side, so a partial transfer is never
    /// visible under the final name to anything else scanning the share.
    /// After the rename, the remote file size is checked against the local
    /// file to catch a connection that dropped after the data transfer but
    /// before the final acknowledgement.
    #[instrument(skip(self), fields(host = %self.host, share = %self.share, remote = %remote_path))]
    pub async fn upload_file(&self, local_path: &Path, remote_path: &str) -> Result<(), SmbError> {
        let local_metadata = tokio::fs::metadata(local_path).await?;
        let expected_size = local_metadata.len();

        let (remote_dir, remote_file) = split_remote_path(remote_path);
        let uploading_name = format!("{remote_file}.uploading");

        let local_dir = local_path
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| ".".to_string());
        let local_file_name = local_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| SmbError::CommandFailed("local path has no file name".to_string()))?;

        // 1. Upload to a temporary remote name.
        let put_cmd = format!(
            "lcd {}; cd {}; put {} {}",
            quote_smb_arg(&local_dir),
            quote_smb_arg(&remote_dir),
            quote_smb_arg(local_file_name),
            quote_smb_arg(&uploading_name),
        );
        self.run_command(&put_cmd).await?;

        // 2. Verify the uploaded size before publishing it under its final name.
        let remote_size = self.remote_file_size(&remote_dir, &uploading_name).await?;
        if remote_size != expected_size {
            let _ = self.delete_remote(&remote_dir, &uploading_name).await;
            return Err(SmbError::SizeMismatch {
                expected: expected_size,
                actual: remote_size,
            });
        }

        // 3. Atomically publish under the final name.
        let rename_cmd = format!(
            "cd {}; rename {} {}",
            quote_smb_arg(&remote_dir),
            quote_smb_arg(&uploading_name),
            quote_smb_arg(&remote_file),
        );
        self.run_command(&rename_cmd).await?;

        info!(remote = %remote_path, size_bytes = expected_size, "upload complete and verified");
        Ok(())
    }

    async fn remote_file_size(&self, remote_dir: &str, file_name: &str) -> Result<u64, SmbError> {
        let cmd = format!(
            "cd {}; ls {}",
            quote_smb_arg(remote_dir),
            quote_smb_arg(file_name)
        );
        let output = self.run_command(&cmd).await?;
        parse_ls_size(&output, file_name)
            .ok_or_else(|| SmbError::NotFound(format!("{remote_dir}/{file_name}")))
    }

    async fn delete_remote(&self, remote_dir: &str, file_name: &str) -> Result<(), SmbError> {
        let cmd = format!(
            "cd {}; rm {}",
            quote_smb_arg(remote_dir),
            quote_smb_arg(file_name)
        );
        self.run_command(&cmd).await?;
        Ok(())
    }
}

/// Available disk space (in bytes) on the filesystem containing `path`, via
/// `df -Pk` (POSIX output format, portable across macOS/Linux).
async fn available_disk_space(path: &Path) -> Result<u64, SmbError> {
    let output = Command::new("df").arg("-Pk").arg(path).output().await?;
    if !output.status.success() {
        return Err(SmbError::CommandFailed(
            String::from_utf8_lossy(&output.stderr).to_string(),
        ));
    }
    parse_df_output(&String::from_utf8_lossy(&output.stdout))
        .ok_or_else(|| SmbError::CommandFailed("failed to parse `df` output".to_string()))
}

/// Parse the "Available" column (in bytes) out of `df -P` POSIX-format
/// output:
/// ```text
/// Filesystem     1024-blocks      Used Available Capacity Mounted on
/// /dev/disk3s5     976490568 123456789 456789012      22%   /
/// ```
fn parse_df_output(output: &str) -> Option<u64> {
    let data_line = output.lines().nth(1)?;
    let fields: Vec<&str> = data_line.split_whitespace().collect();
    let available_kb: u64 = fields.get(3)?.parse().ok()?;
    Some(available_kb * 1024)
}

fn local_dir_str(path: &Path) -> Result<String, SmbError> {
    path.parent()
        .map(|p| {
            if p.as_os_str().is_empty() {
                ".".to_string()
            } else {
                p.to_string_lossy().to_string()
            }
        })
        .ok_or_else(|| SmbError::CommandFailed("local path has no parent directory".to_string()))
}

/// smbclient's `-c` argument is itself a tiny command line that it parses
/// with its own quoting rules (double quotes, backslash escapes). Never
/// concatenate a raw path into that string — always go through this so
/// spaces/quotes in filenames survive.
fn quote_smb_arg(s: &str) -> String {
    let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

/// Split a share-relative path into (directory, filename), both suitable
/// for `cd`/`get`/`put`. `""`/`"."` means "share root".
fn split_remote_path(remote_path: &str) -> (String, String) {
    let trimmed = remote_path.trim_start_matches('/');
    match trimmed.rsplit_once('/') {
        Some((dir, file)) => (dir.to_string(), file.to_string()),
        None => (".".to_string(), trimmed.to_string()),
    }
}

fn join_remote(dir: &str, file: &str) -> String {
    if dir.is_empty() || dir == "." {
        file.to_string()
    } else {
        format!("{}/{}", dir.trim_end_matches('/'), file)
    }
}

fn part_path_for(local_path: &Path) -> PathBuf {
    let mut os_string = local_path.as_os_str().to_os_string();
    os_string.push(".part");
    PathBuf::from(os_string)
}

/// Redact the NAS password from smbclient's output before it is ever logged
/// or turned into an error message.
fn sanitize_smb_output(output: &str, username: &str, password: Option<&str>) -> String {
    let mut sanitized = output.to_string();
    if let Some(password) = password {
        if !password.is_empty() {
            sanitized = sanitized.replace(password, "***REDACTED***");
            sanitized = sanitized.replace(&format!("{username}%{password}"), "***REDACTED***");
        }
    }
    sanitized
}

fn contains_smb_error(output: &str) -> bool {
    // smbclient's non-interactive `-c` mode often still exits 0 even when a
    // sub-command failed, printing an `NT_STATUS_*` line instead (anything
    // other than the implicit success of no output at all). It never prints
    // `NT_STATUS_OK` for a successful op, so any `NT_STATUS_` marker here
    // means something went wrong.
    output.to_lowercase().contains("nt_status_")
}

fn classify_smb_error(output: &str, host: &str, share: &str, username: &str) -> SmbError {
    let lower = output.to_lowercase();
    if lower.contains("nt_status_logon_failure") || lower.contains("nt_status_access_denied") {
        SmbError::AuthFailed(username.to_string())
    } else if lower.contains("nt_status_object_name_not_found")
        || lower.contains("nt_status_no_such_file")
        || lower.contains("nt_status_object_path_not_found")
    {
        SmbError::NotFound(output.to_string())
    } else if lower.contains("nt_status_object_name_collision") {
        SmbError::RemotePathConflict(output.to_string())
    } else if lower.contains("nt_status_connection_reset")
        || lower.contains("nt_status_io_timeout")
        || lower.contains("nt_status_unexpected_network_error")
    {
        SmbError::SessionExpired(output.to_string())
    } else if lower.contains("connection to")
        || lower.contains("couldn't establish connection")
        || lower.contains("unable to connect")
    {
        SmbError::ConnectionFailed(host.to_string(), share.to_string(), output.to_string())
    } else {
        SmbError::CommandFailed(output.to_string())
    }
}

/// Parse the file listing produced by smbclient's `ls` command, e.g.:
/// ```text
///   video1.mp4                          A   1234567  Mon Jan  1 00:00:00 2024
///   video2.mkv                          A   7654321  Mon Jan  1 00:00:00 2024
/// ```
fn parse_ls_output(output: &str) -> Vec<String> {
    output
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty()
                || trimmed.starts_with("NT_STATUS")
                || trimmed.contains("blocks of size")
            {
                return None;
            }
            // smbclient right-aligns "  <attrs>  <size>  <date>" after the
            // name, padded to a fixed column. The name is everything before
            // that block of at least two spaces.
            let name = extract_ls_name(trimmed)?;
            // Skip `.` / `..` entries.
            if name == "." || name == ".." {
                return None;
            }
            Some(name)
        })
        .collect()
}

fn extract_ls_name(line: &str) -> Option<String> {
    // Find the run of 2+ spaces that separates the name column from the
    // attribute column, then take everything before it.
    let bytes = line.as_bytes();
    let mut i = 0;
    while i + 1 < bytes.len() {
        if bytes[i] == b' ' && bytes[i + 1] == b' ' {
            let name = line[..i].trim();
            if name.is_empty() {
                return None;
            }
            return Some(name.to_string());
        }
        i += 1;
    }
    None
}

/// Parse the size (3rd whitespace-separated field after the double-space
/// gap) for `file_name` out of an `ls <file_name>` smbclient output.
fn parse_ls_size(output: &str, file_name: &str) -> Option<u64> {
    output.lines().find_map(|line| {
        let trimmed = line.trim();
        if !trimmed.starts_with(file_name) {
            return None;
        }
        let rest = trimmed[file_name.len()..].trim();
        // rest looks like: "A   1234567  Mon Jan  1 00:00:00 2024"
        let mut fields = rest.split_whitespace();
        let _attrs = fields.next()?;
        let size_str = fields.next()?;
        size_str.parse::<u64>().ok()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_smb_arg_wraps_in_quotes() {
        assert_eq!(quote_smb_arg("simple.mp4"), "\"simple.mp4\"");
    }

    #[test]
    fn test_quote_smb_arg_escapes_spaces_and_quotes() {
        let quoted = quote_smb_arg("my \"video\" file.mp4");
        assert_eq!(quoted, "\"my \\\"video\\\" file.mp4\"");
    }

    #[test]
    fn test_quote_smb_arg_handles_utf8() {
        let quoted = quote_smb_arg("vidéo_日本語.mp4");
        assert_eq!(quoted, "\"vidéo_日本語.mp4\"");
    }

    #[test]
    fn test_split_remote_path_nested() {
        assert_eq!(
            split_remote_path("movies/2024/video.mp4"),
            ("movies/2024".to_string(), "video.mp4".to_string())
        );
    }

    #[test]
    fn test_split_remote_path_root_level() {
        assert_eq!(
            split_remote_path("video.mp4"),
            (".".to_string(), "video.mp4".to_string())
        );
    }

    #[test]
    fn test_split_remote_path_leading_slash() {
        assert_eq!(
            split_remote_path("/video.mp4"),
            (".".to_string(), "video.mp4".to_string())
        );
    }

    #[test]
    fn test_part_path_for_appends_part_extension() {
        let path = Path::new("/tmp/work/video.mp4");
        assert_eq!(
            part_path_for(path),
            PathBuf::from("/tmp/work/video.mp4.part")
        );
    }

    #[test]
    fn test_sanitize_smb_output_redacts_password() {
        let output = "smbclient //host/share -U user%hunter2\nNT_STATUS_LOGON_FAILURE";
        let sanitized = sanitize_smb_output(output, "user", Some("hunter2"));
        assert!(!sanitized.contains("hunter2"));
    }

    #[test]
    fn test_sanitize_smb_output_no_password() {
        let output = "some output";
        let sanitized = sanitize_smb_output(output, "user", None);
        assert_eq!(sanitized, "some output");
    }

    #[test]
    fn test_classify_smb_error_auth_failure() {
        let err = classify_smb_error("NT_STATUS_LOGON_FAILURE", "host", "share", "user");
        assert!(matches!(err, SmbError::AuthFailed(_)));
    }

    #[test]
    fn test_classify_smb_error_not_found() {
        let err = classify_smb_error(
            "NT_STATUS_OBJECT_NAME_NOT_FOUND listing \\video.mp4",
            "host",
            "share",
            "user",
        );
        assert!(matches!(err, SmbError::NotFound(_)));
    }

    #[test]
    fn test_classify_smb_error_connection_failed() {
        let err = classify_smb_error(
            "Connection to host failed (Error NT_STATUS_UNSUCCESSFUL)",
            "host",
            "share",
            "user",
        );
        assert!(matches!(err, SmbError::ConnectionFailed(_, _, _)));
    }

    #[test]
    fn test_parse_ls_output_extracts_filenames() {
        let output = "\n\
  .                                   D        0  Mon Jan  1 00:00:00 2024\n\
  ..                                  D        0  Mon Jan  1 00:00:00 2024\n\
  video1.mp4                         A  1234567  Mon Jan  1 00:00:00 2024\n\
  my video 2.mkv                     A  7654321  Mon Jan  1 00:00:00 2024\n\
\n\
\t\t9999999 blocks of size 1024. 5000000 blocks available\n";
        let names = parse_ls_output(output);
        assert_eq!(names, vec!["video1.mp4", "my video 2.mkv"]);
    }

    #[test]
    fn test_parse_ls_size() {
        let output = "  video1.mp4                         A  1234567  Mon Jan  1 00:00:00 2024\n";
        assert_eq!(parse_ls_size(output, "video1.mp4"), Some(1234567));
    }

    #[test]
    fn test_parse_ls_size_missing_file_returns_none() {
        let output = "  other.mp4                         A  1234567  Mon Jan  1 00:00:00 2024\n";
        assert_eq!(parse_ls_size(output, "video1.mp4"), None);
    }

    #[test]
    fn test_parse_df_output() {
        let output = "Filesystem     1024-blocks      Used  Available Capacity  Mounted on\n\
/dev/disk3s5     976490568 123456789  456789012      22%   /\n";
        assert_eq!(parse_df_output(output), Some(456_789_012 * 1024));
    }

    #[test]
    fn test_parse_df_output_malformed_returns_none() {
        assert_eq!(parse_df_output("not a df output"), None);
    }

    #[tokio::test]
    async fn test_available_disk_space_real_filesystem() {
        // `df` is a standard POSIX utility available in this dev/CI
        // environment; unlike smbclient/ffmpeg this isn't optional tooling,
        // so we exercise the real command here instead of just the parser.
        let dir = tempfile::tempdir().unwrap();
        let space = available_disk_space(dir.path()).await.unwrap();
        assert!(space > 0);
    }
}
