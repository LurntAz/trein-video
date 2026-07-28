use crate::db::{Repository, Video};
use crate::nas::SmbClient;
use chrono::Utc;
use std::sync::Arc;
use tracing::{info, warn};

const VIDEO_EXTENSIONS: &[&str] = &["mkv", "mp4", "avi", "mov", "flv", "wmv", "webm"];

pub struct VideoDiscovery {
    smb_client: Arc<SmbClient>,
    repository: Arc<Repository>,
    base_path: String,
}

impl VideoDiscovery {
    pub fn new(smb_client: Arc<SmbClient>, repository: Arc<Repository>, base_path: String) -> Self {
        Self {
            smb_client,
            repository,
            base_path,
        }
    }

    /// Recursively scan the NAS folder and discover new video files.
    /// Optimized: loads all existing videos once, then batch-inserts new ones.
    /// Returns the count of newly discovered (and inserted) videos.
    pub async fn discover_videos(&self) -> Result<usize, String> {
        info!("Starting video discovery in {}", self.base_path);

        let start = std::time::Instant::now();

        let files = self
            .list_files_recursive(&self.base_path)
            .await
            .map_err(|e| format!("Failed to list NAS files: {}", e))?;

        info!(
            "Found {} total files in {} (took {:?})",
            files.len(),
            self.base_path,
            start.elapsed()
        );

        // Filter to video files only
        let video_files: Vec<String> = files
            .into_iter()
            .filter(|f| self.is_video_file(f))
            .collect();

        info!("Filtered to {} video files", video_files.len());

        // Load all existing videos once (bulk operation)
        let existing_ids = self
            .repository
            .get_all_video_ids()
            .await
            .map_err(|e| format!("Failed to load existing videos: {}", e))?;

        let existing_set: std::collections::HashSet<String> = existing_ids.into_iter().collect();
        info!("Found {} existing videos in DB", existing_set.len());

        // Collect only new videos
        let new_videos: Vec<Video> = video_files
            .into_iter()
            .filter(|path| !existing_set.contains(path))
            .map(|file_path| Video {
                id: file_path.clone(),
                file_path,
                status: "pending".to_string(),
                original_codec: None,
                original_bitrate_kbps: None,
                original_size_bytes: None,
                converted_size_bytes: None,
                instance_id: None,
                error_message: None,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                claimed_at: None,
                attempts: 0,
                last_retry_time: None,
            })
            .collect();

        let discovered = new_videos.len();
        if discovered == 0 {
            info!("No new videos discovered");
            return Ok(0);
        }

        info!("Inserting {} new videos", discovered);
        for video in new_videos {
            if let Err(e) = self.repository.insert_video(&video).await {
                warn!("Failed to insert video {}: {}", video.id, e);
            }
        }

        info!(
            "Discovered {} new videos in {:?}",
            discovered,
            start.elapsed()
        );
        Ok(discovered)
    }

    /// List all files recursively in a folder using smbclient.
    async fn list_files_recursive(&self, folder: &str) -> Result<Vec<String>, String> {
        use std::process::Stdio;
        use tokio::process::Command;

        let share = format!("//{}/{}", self.smb_client.host, self.smb_client.share);
        let path = if folder.is_empty() || folder == "/" {
            "\\".to_string()
        } else {
            folder.replace('/', "\\")
        };

        // Try to get password from Option or from environment
        let password = self.smb_client.password.clone().or_else(|| {
            // If password is None, we may need to check env vars
            // But this is already handled by the config loader via get_password()
            None
        });

        let userpass = match password {
            Some(pwd) => format!("{}%{}", self.smb_client.username, pwd),
            None => self.smb_client.username.clone(),
        };

        let output = Command::new("smbclient")
            .arg(&share)
            .arg("-U")
            .arg(&userpass)
            .arg("-c")
            .arg(format!(r#"recurse ON; ls "{}""#, path))
            .stdin(Stdio::null())
            .output()
            .await
            .map_err(|e| format!("smbclient failed: {}", e))?;

        let stdout = String::from_utf8_lossy(&output.stdout);

        info!(
            "list_files_recursive: raw smbclient output ({} bytes):\n{}",
            stdout.len(),
            &stdout
        );

        // smbclient may return non-zero exit code due to config warnings,
        // but still provide output. Only fail if there's no output AND bad status.
        if !output.status.success() && stdout.is_empty() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("smbclient error: {}", stderr));
        }

        let files = parse_smbclient_ls(&stdout, folder);
        info!("list_files_recursive: parsed {} total files", files.len());

        // Log first 10 files for debugging
        for (i, file) in files.iter().take(10).enumerate() {
            info!("  [{}] {}", i + 1, file);
        }

        Ok(files)
    }

    /// Check if a file has a video extension.
    fn is_video_file(&self, path: &str) -> bool {
        let lower = path.to_lowercase();
        VIDEO_EXTENSIONS
            .iter()
            .any(|ext| lower.ends_with(&format!(".{}", ext)))
    }

    /// Insert the video if it doesn't already exist (by file_path).
    /// Returns Ok(true) if inserted, Ok(false) if already exists.
    async fn insert_if_new(&self, file_path: &str) -> Result<bool, String> {
        let existing = self
            .repository
            .get_video(file_path)
            .await
            .map_err(|e| format!("DB error: {}", e))?;

        if existing.is_some() {
            return Ok(false);
        }

        let video = Video {
            id: file_path.to_string(),
            file_path: file_path.to_string(),
            status: "pending".to_string(),
            original_codec: None,
            original_bitrate_kbps: None,
            original_size_bytes: None,
            converted_size_bytes: None,
            instance_id: None,
            error_message: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            claimed_at: None,
            attempts: 0,
            last_retry_time: None,
        };

        self.repository
            .insert_video(&video)
            .await
            .map_err(|e| format!("Failed to insert video: {}", e))?;

        info!("Discovered new video: {}", file_path);
        Ok(true)
    }
}

/// Parse smbclient `ls -R` output to extract file paths.
/// Example output:
///   \Omu
///           .                                   D        0  Sat Jul 26 19:00:00 2026
///           ..                                  D        0  Sat Jul 26 19:00:00 2026
///           test.mkv                            A  5242880  Sat Jul 26 19:00:00 2026
fn parse_smbclient_ls(output: &str, base_path: &str) -> Vec<String> {
    let mut files = Vec::new();
    let mut current_folder = base_path.to_string();

    for line in output.lines() {
        let trimmed = line.trim();

        // Skip empty lines, headers, and meta entries
        if trimmed.is_empty()
            || trimmed == "."
            || trimmed == ".."
            || trimmed.starts_with("blocks of size")
            || trimmed.starts_with("WARNING:")
        {
            continue;
        }

        // Detect folder headers (lines starting with \)
        if trimmed.starts_with('\\') {
            // Convert \ to / for consistency with the rest of the codebase
            current_folder = trimmed.replace('\\', "/");
            continue;
        }

        // Parse file entries: name followed by flags and size
        // Format: "name <spaces> [A|D|AH] <spaces> size <spaces> date..."
        // Split by whitespace and find the flag (A, D, or AH)
        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        // Find the flag: look for "A", "D", or "AH" in the chunks
        if let Some(flag_idx) = parts
            .iter()
            .position(|p| *p == "A" || *p == "D" || *p == "AH")
        {
            let flag = parts[flag_idx];

            // Skip directories (flag == "D")
            if flag == "D" {
                continue;
            }

            // Everything before the flag is the filename (handles names with spaces)
            let name_part = parts[..flag_idx].join(" ");

            let file_path = if current_folder.is_empty() || current_folder == "/" {
                name_part
            } else {
                format!("{}/{}", current_folder, name_part)
            };
            files.push(file_path);
        }
    }

    files
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_video_file() {
        let discovery = VideoDiscovery {
            smb_client: Arc::new(SmbClient::new(
                "192.168.1.100".to_string(),
                "videos".to_string(),
                "user".to_string(),
                Some("pass".to_string()),
            )),
            repository: Arc::new(crate::db::Repository::new(
                sqlx::sqlite::SqlitePool::default(),
            )),
            base_path: "/videos".to_string(),
        };

        assert!(discovery.is_video_file("video.mkv"));
        assert!(discovery.is_video_file("VIDEO.MKV"));
        assert!(discovery.is_video_file("/path/to/video.mp4"));
        assert!(!discovery.is_video_file("document.pdf"));
        assert!(!discovery.is_video_file("archive.zip"));
    }

    #[test]
    fn test_parse_smbclient_ls_empty() {
        let output = "";
        let files = parse_smbclient_ls(output, "/videos");
        assert!(files.is_empty());
    }

    #[test]
    fn test_parse_smbclient_ls_with_files() {
        let output = r#"
\Omu
        .                                   D        0  Sat Jul 26 19:00:00 2026
        ..                                  D        0  Sat Jul 26 19:00:00 2026
        test.mkv                            A  5242880  Sat Jul 26 19:00:00 2026
        document.pdf                        A  1048576  Sat Jul 26 19:00:00 2026

blocks of size 1024. 1234567 blocks available
        "#;

        let files = parse_smbclient_ls(output, "/Omu");
        assert_eq!(files.len(), 2);
        assert!(files.iter().any(|f| f.contains("test.mkv")));
        assert!(files.iter().any(|f| f.contains("document.pdf")));
    }
}
