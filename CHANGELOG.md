# Changelog

All notable changes to Trein Video will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-07-28

### Added

#### Auto Video Discovery (#21)
- Recursive NAS folder scanning on master node
- Configurable scan interval (`video_discovery.interval_secs`, default 3600s)
- Efficient bulk database operations (load existing videos once, batch insert new ones)
- Handles folders with thousands of videos without performance degradation
- Supports multiple video formats (.mkv, .mp4, .avi, .mov, .flv, .wmv, .webm)

#### Discord Notifications (#22, #23, #24)
- Real-time conversion status webhooks sent to Discord channel
- Embed messages with:
  - Video filename and file size (human-readable MB/GB format)
  - Conversion duration (HH:MM:SS)
  - Success/failure status with color coding (green/red)
  - Error messages on failure
- Non-blocking fire-and-forget delivery (no impact on conversion pipeline)
- Optional configuration via `discord.enabled` and `discord.webhook_url`

#### Core Features
- Master-worker distributed architecture with mTLS
- Atomic job queue with SQLite transactions
- Automatic mDNS service discovery
- SMB/CIFS NAS integration (download, analyze, convert, upload)
- Real-time progress tracking with live encoding statistics
- Adaptive encoding parameters (CRF/preset auto-tuning by file size)
- Exponential backoff retry logic with 5 configurable attempts
- Stale job reclamation for crashed workers
- Structured JSON/human-readable logging via `tracing`
- Comprehensive test suite (150+ unit and integration tests)

### Fixed

- FFmpeg timeout removed: encodings can now run indefinitely (no 30-minute limit)
- Tilde (`~`) expansion in config paths: now correctly resolves to home directory instead of creating literal `~` folder
- Path normalization between Windows SMB format and Unix conventions
- AV1 numeric preset handling: correctly passes through preset values 0-13 without mapping

### Technical Details

- **Language**: Rust 1.70+
- **Async Runtime**: Tokio with structured concurrency
- **Database**: SQLite with WAL mode
- **Video Processing**: ffmpeg/ffprobe CLI integration
- **Network**: mTLS with self-signed certificate generation
- **Service Discovery**: mDNS/Zeroconf
- **Encoding**: libsvtav1 (AV1), libx265 (H.265)

### Performance

- Single worker: 1.5 GB x265 → AV1 preset 4 = 3-4 hours (M1 Mac 2020)
- Scales linearly with parallel workers
- Bulk video discovery: 37 videos in ~1-2 minutes
- Efficient database operations: HashSet-based duplicate filtering

### Known Limitations

- SMB/CIFS protocol only (no local filesystem, SFTP, or S3)
- AV1 encoding slower than H.265 (~0.2-0.5x realtime speed on M1)
- Requires network connectivity to NAS for all operations
- Single master (no clustering/failover for master itself)

---

**Note**: This is the initial release. For detailed ticket tracking, see `.tkt/` directory.
