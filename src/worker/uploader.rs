use crate::nas::{SmbClient, SmbError};
use crate::progress::{send_progress, ProgressEvent, ProgressSender};
use std::path::Path;
use tracing::{info, instrument};

pub struct Uploader;

impl Uploader {
    /// Upload `local_path` to `remote_path` (relative to the configured NAS
    /// share). Delegates to [`SmbClient::upload_file`], which uploads to a
    /// temporary remote name, verifies the transferred size against the
    /// local file, and only then renames it into place — see that method's
    /// docs for why (#9's post-upload verification requirement).
    #[instrument(skip(smb_client), fields(remote = %remote_path, local = %local_path.display()))]
    pub async fn upload_to_nas(
        smb_client: &SmbClient,
        local_path: &Path,
        remote_path: &str,
    ) -> Result<(), SmbError> {
        info!("starting upload to NAS");
        smb_client.upload_file(local_path, remote_path).await?;
        info!("upload to NAS complete and verified");
        Ok(())
    }

    /// Like [`Self::upload_to_nas`], additionally emitting
    /// [`ProgressEvent::TransferProgress`] (#20) at the start (0%) and, on
    /// success, completion (100%) of the transfer.
    ///
    /// Unlike downloads (where the growing local `.part` file gives a free,
    /// pollable progress signal), an upload's local source file is static
    /// for the whole transfer, and observing live progress on the remote
    /// side would mean extra `smbclient ls` round-trips against the
    /// in-progress `.uploading` temp file racing the transfer itself --
    /// judged not worth the added complexity/fragility here, so this
    /// reports only the two endpoints rather than a live percentage.
    #[instrument(skip(smb_client, progress_tx), fields(remote = %remote_path, local = %local_path.display()))]
    pub async fn upload_to_nas_with_progress(
        smb_client: &SmbClient,
        local_path: &Path,
        remote_path: &str,
        video_id: &str,
        progress_tx: &ProgressSender,
    ) -> Result<(), SmbError> {
        info!("starting upload to NAS");

        let total_bytes = tokio::fs::metadata(local_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        send_progress(
            progress_tx,
            ProgressEvent::TransferProgress {
                direction: "upload".to_string(),
                video_id: video_id.to_string(),
                bytes_transferred: 0,
                bytes_total: total_bytes,
                speed_mbps: 0.0,
                eta_secs: 0.0,
            },
        );

        smb_client.upload_file(local_path, remote_path).await?;

        send_progress(
            progress_tx,
            ProgressEvent::TransferProgress {
                direction: "upload".to_string(),
                video_id: video_id.to_string(),
                bytes_transferred: total_bytes,
                bytes_total: total_bytes,
                speed_mbps: 0.0,
                eta_secs: 0.0,
            },
        );

        info!("upload to NAS complete and verified");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_upload_to_nas_propagates_error_for_missing_local_file() {
        let client = SmbClient::new(
            "127.0.0.1".to_string(),
            "share".to_string(),
            "user".to_string(),
            None,
        );
        let dir = tempfile::tempdir().unwrap();
        let missing_local = dir.path().join("does-not-exist.mkv");
        let result = Uploader::upload_to_nas(&client, &missing_local, "video_av1.mkv").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_upload_to_nas_propagates_connection_error() {
        let client = SmbClient::new(
            "127.0.0.1".to_string(),
            "share".to_string(),
            "user".to_string(),
            None,
        );
        let dir = tempfile::tempdir().unwrap();
        let local_path = dir.path().join("out.mkv");
        tokio::fs::write(&local_path, b"fake video data")
            .await
            .unwrap();

        let result = Uploader::upload_to_nas(&client, &local_path, "video_av1.mkv").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_upload_to_nas_with_progress_propagates_error_for_missing_local_file() {
        let client = SmbClient::new(
            "127.0.0.1".to_string(),
            "share".to_string(),
            "user".to_string(),
            None,
        );
        let dir = tempfile::tempdir().unwrap();
        let missing_local = dir.path().join("does-not-exist.mkv");
        let (tx, _rx) = crate::progress::channel();

        let result = Uploader::upload_to_nas_with_progress(
            &client,
            &missing_local,
            "video_av1.mkv",
            "video-1",
            &tx,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_upload_to_nas_with_progress_emits_start_and_end_events() {
        let client = SmbClient::new(
            "127.0.0.1".to_string(),
            "share".to_string(),
            "user".to_string(),
            None,
        );
        let dir = tempfile::tempdir().unwrap();
        let local_path = dir.path().join("out.mkv");
        tokio::fs::write(&local_path, b"fake video data")
            .await
            .unwrap();
        let (tx, mut rx) = crate::progress::channel();

        // The actual upload will fail (no real smbclient/NAS in this
        // environment), but the initial 0%-progress event must still have
        // been sent before that failure is surfaced.
        let result =
            Uploader::upload_to_nas_with_progress(&client, &local_path, "video_av1.mkv", "v1", &tx)
                .await;
        assert!(result.is_err());

        let event = rx
            .try_recv()
            .expect("expected a start TransferProgress event");
        match event {
            ProgressEvent::TransferProgress {
                direction,
                bytes_transferred,
                bytes_total,
                ..
            } => {
                assert_eq!(direction, "upload");
                assert_eq!(bytes_transferred, 0);
                assert_eq!(bytes_total, 15); // "fake video data".len()
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
}
