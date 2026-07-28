//! Discord webhook notifications.
//!
//! [`DiscordNotifier`] posts a single rich-embed message to a Discord
//! incoming webhook URL each time a video conversion finishes (success or
//! failure). It is meant to be called fire-and-forget from the worker
//! processor after each job (see `src/worker/processor.rs`) -- a Discord
//! outage or a bad webhook URL must never fail or block the conversion
//! pipeline itself, so every fallible step here is caught and turned into a
//! logged `Err(String)` rather than a panic.

use serde_json::json;
use tracing::warn;

/// Sends "conversion complete"/"conversion failed" notifications to a single
/// Discord channel via an incoming webhook.
///
/// Cheap to construct and clone-free to hold behind an `Arc` (it owns just
/// the webhook URL and a `reqwest::Client`, which is itself internally
/// pooled/`Arc`-backed), so one instance can be shared across all worker
/// jobs.
pub struct DiscordNotifier {
    webhook_url: String,
    client: reqwest::Client,
}

impl DiscordNotifier {
    pub fn new(webhook_url: String) -> Self {
        Self {
            webhook_url,
            client: reqwest::Client::new(),
        }
    }

    /// Post a conversion-result embed to the configured webhook.
    ///
    /// `video_id` is typically the NAS remote path (e.g.
    /// `"movies/foo.mp4"`); only its filename component is shown. Errors
    /// (a network failure, a non-2xx response from Discord, ...) are
    /// returned rather than panicking -- callers running this
    /// fire-and-forget from the worker should log the `Err` and move on
    /// rather than let a notification failure affect job status.
    pub async fn send_conversion_complete(
        &self,
        video_id: &str,
        file_size_bytes: u64,
        duration_secs: u64,
        success: bool,
        error_message: Option<&str>,
    ) -> Result<(), String> {
        let payload = build_payload(
            video_id,
            file_size_bytes,
            duration_secs,
            success,
            error_message,
        );

        let response = self
            .client
            .post(&self.webhook_url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| {
                let msg = format!("failed to send Discord notification: {e}");
                warn!(error = %e, video_id, "discord webhook request failed");
                msg
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            let msg = format!("Discord webhook returned {status}: {body}");
            warn!(video_id, status = %status, body = %body, "discord webhook returned non-success status");
            return Err(msg);
        }

        Ok(())
    }
}

/// Build the JSON body for Discord's webhook execute endpoint: a single
/// embed with a title, color, timestamp, and one field per piece of
/// conversion info. See
/// <https://discord.com/developers/docs/resources/webhook#execute-webhook>.
fn build_payload(
    video_id: &str,
    file_size_bytes: u64,
    duration_secs: u64,
    success: bool,
    error_message: Option<&str>,
) -> serde_json::Value {
    let title = if success {
        "\u{2705} Video Converted"
    } else {
        "\u{274c} Conversion Failed"
    };
    let color = if success { 0x00FF00 } else { 0xFF0000 };

    let mut fields = vec![
        json!({
            "name": "Video",
            "value": file_name_of(video_id),
            "inline": true,
        }),
        json!({
            "name": "Size",
            "value": format_bytes(file_size_bytes),
            "inline": true,
        }),
        json!({
            "name": "Time",
            "value": format_duration(duration_secs),
            "inline": true,
        }),
    ];
    if let Some(err) = error_message {
        fields.push(json!({
            "name": "Error",
            "value": err,
            "inline": false,
        }));
    }

    json!({
        "embeds": [{
            "title": title,
            "color": color,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "fields": fields,
        }]
    })
}

/// `movies/foo.mp4` -> `foo.mp4` (mirrors
/// `worker::processor::file_name_of`; duplicated here rather than shared
/// since this module must stay independent of `worker`).
fn file_name_of(video_id: &str) -> String {
    video_id.rsplit('/').next().unwrap_or(video_id).to_string()
}

/// Format a byte count as a human-readable size using binary (1024-based)
/// units, e.g. `847` -> `"847 B"`, `888_234_291` -> `"847 MB"`,
/// `1_288_490_189` -> `"1.2 GB"`. MB/GB values are rounded to one decimal
/// place with a trailing `.0` trimmed (matching the `"847 MB"` example in
/// the ticket, not `"847.0 MB"`).
fn format_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;

    let bytes_f = bytes as f64;
    if bytes_f >= GB {
        format!("{} GB", trim_trailing_zero(bytes_f / GB))
    } else if bytes_f >= MB {
        format!("{} MB", trim_trailing_zero(bytes_f / MB))
    } else if bytes_f >= KB {
        format!("{} KB", trim_trailing_zero(bytes_f / KB))
    } else {
        format!("{bytes} B")
    }
}

/// `1.0` -> `"1"`, `1.2` -> `"1.2"`. Always rounds to one decimal place
/// first.
fn trim_trailing_zero(value: f64) -> String {
    let rounded = (value * 10.0).round() / 10.0;
    if rounded.fract() == 0.0 {
        format!("{rounded:.0}")
    } else {
        format!("{rounded:.1}")
    }
}

/// Format a duration in seconds as `HH:MM:SS`, e.g. `3725` -> `"01:02:05"`.
fn format_duration(secs: u64) -> String {
    let hours = secs / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes_under_1kb() {
        assert_eq!(format_bytes(512), "512 B");
    }

    #[test]
    fn test_format_bytes_kb() {
        assert_eq!(format_bytes(2048), "2 KB");
    }

    #[test]
    fn test_format_bytes_mb_whole() {
        // 847 MB, matching the ticket's example.
        let bytes = (847.0 * 1024.0 * 1024.0) as u64;
        assert_eq!(format_bytes(bytes), "847 MB");
    }

    #[test]
    fn test_format_bytes_gb_fractional() {
        let bytes = (1.2 * 1024.0 * 1024.0 * 1024.0) as u64;
        assert_eq!(format_bytes(bytes), "1.2 GB");
    }

    #[test]
    fn test_format_duration_zero() {
        assert_eq!(format_duration(0), "00:00:00");
    }

    #[test]
    fn test_format_duration_under_a_minute() {
        assert_eq!(format_duration(45), "00:00:45");
    }

    #[test]
    fn test_format_duration_hours_minutes_seconds() {
        assert_eq!(format_duration(3725), "01:02:05");
    }

    #[test]
    fn test_file_name_of_extracts_last_segment() {
        assert_eq!(file_name_of("movies/2024/foo.mp4"), "foo.mp4");
        assert_eq!(file_name_of("foo.mp4"), "foo.mp4");
    }

    #[test]
    fn test_build_payload_success_has_green_color_and_no_error_field() {
        let payload = build_payload("movies/foo.mp4", 1000, 65, true, None);
        let embed = &payload["embeds"][0];
        assert_eq!(embed["title"], "\u{2705} Video Converted");
        assert_eq!(embed["color"], 0x00FF00);
        let fields = embed["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0]["value"], "foo.mp4");
    }

    #[test]
    fn test_build_payload_failure_has_red_color_and_error_field() {
        let payload = build_payload(
            "movies/foo.mp4",
            0,
            12,
            false,
            Some("ffmpeg exited with code 1"),
        );
        let embed = &payload["embeds"][0];
        assert_eq!(embed["title"], "\u{274c} Conversion Failed");
        assert_eq!(embed["color"], 0xFF0000);
        let fields = embed["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[3]["name"], "Error");
        assert_eq!(fields[3]["value"], "ffmpeg exited with code 1");
    }

    #[tokio::test]
    async fn test_send_conversion_complete_posts_to_webhook_url() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/webhook")
            .with_status(204)
            .create_async()
            .await;

        let notifier = DiscordNotifier::new(format!("{}/webhook", server.url()));
        let result = notifier
            .send_conversion_complete("movies/foo.mp4", 1000, 65, true, None)
            .await;

        assert!(result.is_ok(), "{result:?}");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_send_conversion_complete_returns_err_on_non_success_status() {
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/webhook")
            .with_status(400)
            .with_body("bad request")
            .create_async()
            .await;

        let notifier = DiscordNotifier::new(format!("{}/webhook", server.url()));
        let result = notifier
            .send_conversion_complete("movies/foo.mp4", 1000, 65, false, Some("boom"))
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("400"));
    }
}
