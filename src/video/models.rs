use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoMetadata {
    pub codec: String,
    pub bitrate_kbps: u32,
    pub resolution: String,
    pub filesize_bytes: u64,
    pub duration_secs: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversionTask {
    pub id: String,
    pub input_path: String,
    pub output_path: String,
    pub metadata: VideoMetadata,
}
