use crate::nas::{SmbClient, SmbError};
use crate::progress::{send_progress, ProgressEvent, ProgressSender};
use std::path::Path;
use std::time::Instant;
use tracing::{info, instrument};

pub struct Downloader;

impl Downloader {
    /// Download `remote_path` (relative to the configured NAS share) into
    /// `local_path`. Delegates the actual transfer (including the
    /// `.part`-file/atomic-rename safety and size verification) to
    /// [`SmbClient::download_file`] so #8/#9 share one implementation of
    /// the smbclient plumbing instead of duplicating it.
    #[instrument(skip(smb_client), fields(remote = %remote_path, local = %local_path.display()))]
    pub async fn download_from_nas(
        smb_client: &SmbClient,
        remote_path: &str,
        local_path: &Path,
    ) -> Result<(), SmbError> {
        info!("starting download from NAS");
        smb_client.download_file(remote_path, local_path).await?;
        info!("download from NAS complete");
        Ok(())
    }

    /// Like [`Self::download_from_nas`], additionally emitting
    /// [`ProgressEvent::TransferProgress`] events (#20) as the transfer
    /// runs. Speed/ETA are derived here (rather than in [`SmbClient`],
    /// which only reports raw byte counts) from consecutive
    /// `(bytes_transferred, bytes_total)` samples and their wall-clock gap.
    #[instrument(skip(smb_client, progress_tx), fields(remote = %remote_path, local = %local_path.display()))]
    pub async fn download_from_nas_with_progress(
        smb_client: &SmbClient,
        remote_path: &str,
        local_path: &Path,
        video_id: &str,
        progress_tx: &ProgressSender,
    ) -> Result<(), SmbError> {
        info!("starting download from NAS");

        let video_id = video_id.to_string();
        let tx = progress_tx.clone();
        let mut last_sample: Option<(Instant, u64)> = None;

        let on_progress: Box<dyn FnMut(u64, u64) + Send> =
            Box::new(move |transferred: u64, total: u64| {
                let now = Instant::now();
                let (speed_mbps, eta_secs) = match last_sample {
                    Some((prev_time, prev_bytes)) => {
                        let elapsed = now.duration_since(prev_time).as_secs_f32().max(0.001);
                        let delta_bytes = transferred.saturating_sub(prev_bytes) as f32;
                        let speed_bps = (delta_bytes / elapsed).max(0.0);
                        let remaining = total.saturating_sub(transferred) as f32;
                        let eta = if speed_bps > 0.0 {
                            (remaining / speed_bps).max(0.0)
                        } else {
                            0.0
                        };
                        (speed_bps / 1_000_000.0, eta)
                    }
                    // No prior sample yet -- can't compute a rate.
                    None => (0.0, 0.0),
                };
                last_sample = Some((now, transferred));
                send_progress(
                    &tx,
                    ProgressEvent::TransferProgress {
                        direction: "download".to_string(),
                        video_id: video_id.clone(),
                        bytes_transferred: transferred,
                        bytes_total: total,
                        speed_mbps,
                        eta_secs,
                    },
                );
            });

        smb_client
            .download_file_with_progress(remote_path, local_path, Some(on_progress))
            .await?;
        info!("download from NAS complete");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_download_from_nas_propagates_missing_binary_error() {
        // Without a real Samba server/smbclient binary available in this
        // environment, the only behavior we can assert end-to-end is that
        // failures surface as a typed `SmbError` rather than panicking.
        let client = SmbClient::new(
            "127.0.0.1".to_string(),
            "share".to_string(),
            "user".to_string(),
            None,
        );
        let dir = tempfile::tempdir().unwrap();
        let local_path = dir.path().join("out.mp4");
        let result = Downloader::download_from_nas(&client, "video.mp4", &local_path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_download_from_nas_with_progress_propagates_error() {
        let client = SmbClient::new(
            "127.0.0.1".to_string(),
            "share".to_string(),
            "user".to_string(),
            None,
        );
        let dir = tempfile::tempdir().unwrap();
        let local_path = dir.path().join("out.mp4");
        let (tx, _rx) = crate::progress::channel();

        let result = Downloader::download_from_nas_with_progress(
            &client,
            "video.mp4",
            &local_path,
            "video-1",
            &tx,
        )
        .await;
        assert!(result.is_err());
    }
}
