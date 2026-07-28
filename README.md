# Trein Video — Distributed Video Converter

A high-performance, distributed video converter for NAS environments. **Trein Video** converts videos to AV1/H.265 codecs using a master-worker architecture with automatic service discovery, atomic job claiming, and comprehensive error handling.

## Overview

```
┌────────────────┐
│    Master      │  • mTLS HTTP server (port 8443)
│  • Job queue   │  • mDNS advertiser (_trein._tcp)
│  • SQLite DB   │  • REST API for job submission
│  • Sync coord  │  • Worker coordination
└────────────────┘
        ↕ mTLS
    ┌───────────┐
    │  Workers  │  (1...N)
    │ • Download│  • mDNS auto-discovery
    │ • Convert │  • Download from NAS (SMB)
    │ • Upload  │  • Analyze & convert (ffmpeg)
    │ • Sync    │  • Upload back to NAS
    └───────────┘
```

## Features

- **Master-Worker Cluster**: Automatic mDNS service discovery; no manual IP configuration
- **Atomic Job Queue**: Race-condition-free job claiming with SQLite transactions
- **mTLS Security**: Server and client certificate validation; self-signed cert generation on first boot
- **NAS Integration**: SMB/CIFS downloads and uploads with credential redaction
- **Auto Video Discovery**: Recursive NAS folder scanning with efficient bulk database operations
- **Discord Notifications**: Real-time conversion status updates via webhook (success/failure with file size & duration)
- **Real-Time Analysis**: ffprobe integration to detect video codec and resolution
- **Smart Encoding**: Adaptive AV1/H.265 encoding with CRF/preset auto-tuning per file size; unlimited encoding duration
- **Resilient Transfers**: Exponential backoff retry logic; stale job reclamation for crashed workers
- **Structured Logging**: JSON/human-readable output via `tracing` with environment filter
- **Full Test Suite**: 150+ unit + integration tests; no external tool mocking for CLI verification

## Requirements

### System Dependencies

- **Rust**: 1.70+ (install via [rustup](https://rustup.rs/))
- **FFmpeg**: Required for encoding
  ```sh
  # macOS (Homebrew)
  brew install ffmpeg
  
  # Ubuntu/Debian
  sudo apt-get install ffmpeg
  
  # Fedora/RHEL
  sudo dnf install ffmpeg
  ```
- **SMB/CIFS tools**: For NAS access
  ```sh
  # macOS
  brew install samba
  
  # Ubuntu/Debian
  sudo apt-get install smbclient cifs-utils
  
  # Fedora/RHEL
  sudo dnf install samba-client
  ```

### Network Requirements

- **Master**: Network reachable by all workers (port 8443 default)
- **mDNS**: Workers must be on the same local network (multicast enabled) or use explicit `master_url` in config
- **NAS**: SMB/CIFS share accessible by all instances

## Installation

### 1. Clone the Repository

```sh
git clone https://github.com/your-org/trein-video.git
cd trein-video
```

### 2. Build

```sh
# Debug build (fast compilation, slower runtime)
cargo build

# Release build (optimized for production)
cargo build --release
```

Binary location: `target/release/trein-video` (or `target/debug/trein-video`)

### 3. Verify Installation

```sh
# Check binary works
./target/release/trein-video --help

# Run test suite (no ffmpeg/smbclient required)
cargo test

# Run all tests including ffmpeg-dependent ones
cargo test -- --ignored
```

## Configuration

### Generate Example Config

```sh
cp config.example.toml ~/.config/trein-video.toml
```

### Master Node Configuration

```toml
[instance]
id = "master-01"
role = "master"
api_port = 8443

[database]
path = "~/.cache/trein-video/jobs.db"

[nas]
protocol = "smb"
host = "192.168.1.100"
share = "videos"
username = "user"
password_env = "NAS_PASSWORD"  # Load from env instead of config
base_path = "/public/videos"

[conversion]
codec = "av1"          # "av1" or "h265"
preset = "medium"      # libsvtav1: ultrafast..veryslow (see #19)
crf = 30               # Quality 0-63 (lower=better)
max_parallel_jobs = 4

[sync]
poll_interval_secs = 60

[tls]
cert_path = "~/.config/trein-video/certs/server.crt"
key_path = "~/.config/trein-video/certs/server.key"
ca_cert_path = "~/.config/trein-video/certs/ca.crt"

[discovery]
enabled = true
service_name = "trein-video-master"

[video_discovery]
enabled = true
interval_secs = 3600  # Scan NAS every hour for new videos

[discord]
enabled = true
webhook_url = "https://discord.com/api/webhooks/YOUR_WEBHOOK_ID/YOUR_WEBHOOK_TOKEN"

[retry]
max_attempts = 5
base_delay_secs = 1
max_delay_secs = 300  # 5 minutes
```

### Worker Node Configuration

```toml
[instance]
id = "worker-01"
role = "worker"
api_port = 8444

[database]
path = "~/.cache/trein-video/local.db"

[nas]
protocol = "smb"
host = "192.168.1.100"
share = "videos"
username = "user"
password_env = "NAS_PASSWORD"
base_path = "/public/videos"

[conversion]
codec = "av1"
preset = "fast"        # Faster encoding on workers
crf = 32               # Slightly lower quality OK
max_parallel_jobs = 2

[sync]
poll_interval_secs = 30
master_url = "https://192.168.1.10:8443"  # If mDNS unavailable

[tls]
cert_path = "~/.config/trein-video/certs/client.crt"
key_path = "~/.config/trein-video/certs/client.key"
ca_cert_path = "~/.config/trein-video/certs/ca.crt"

[discovery]
enabled = true
service_name = "trein-video-master"

[retry]
max_attempts = 5
base_delay_secs = 2
max_delay_secs = 300
```

### Configuration Notes

- **Passwords**: Use `password_env = "ENV_VAR_NAME"` instead of plain `password` to load from environment
- **Certificates**: Auto-generated on first run if missing; manually update paths if using existing PKI
- **Database**: SQLite; auto-created with migrations if path doesn't exist
- **Video Discovery**: Set `video_discovery.enabled = true` on master; scans NAS folder recursively at configurable interval
- **Discord Webhooks**: Optional; create a Discord server webhook and paste the URL in `discord.webhook_url` to get real-time conversion notifications
- **Logging**: Control via `RUST_LOG` env var (e.g., `RUST_LOG=trein_video=debug,tokio=info`)

## Running

### Master Node

```sh
# Set environment variables
export NAS_PASSWORD="your-nas-password"
export RUST_LOG="trein_video=info"

# Run master
./target/release/trein-video \
  --config ~/.config/trein-video.toml \
  --role master

# Or with all args overridden
./target/release/trein-video \
  --config ~/.config/trein-video.toml \
  --role master \
  --instance-id master-prod-01
```

### Worker Nodes (on separate machines or processes)

```sh
# Set environment
export NAS_PASSWORD="your-nas-password"
export RUST_LOG="trein_video=info"

# Run worker
./target/release/trein-video \
  --config ~/.config/trein-video-worker.toml \
  --role worker

# Multiple workers on same machine (different config files)
./target/release/trein-video --config ~/.config/trein-video-worker-1.toml &
./target/release/trein-video --config ~/.config/trein-video-worker-2.toml &
```

### Logs

By default, logs go to stdout in pretty-printed format. To enable JSON logging:

```sh
export LOG_FORMAT=json
./target/release/trein-video --config config.toml
```

## API

The master exposes a REST API on `https://[bind]:8443` (mTLS required).

### Endpoints

```
GET  /health                          # Health check
GET  /api/status                      # Master/worker status
POST /api/submit                      # Submit video for conversion
GET  /api/jobs                        # List all jobs
GET  /api/jobs/<id>                   # Get job status
POST /api/jobs/<id>/status            # Update job status (workers)
GET  /api/instances                   # List connected workers
POST /api/instances/heartbeat         # Worker heartbeat
```

### Example: Submit a Video

```sh
# Generate mTLS client cert (or reuse existing)
curl --cacert ~/.config/trein-video/certs/ca.crt \
     --cert ~/.config/trein-video/certs/client.crt \
     --key ~/.config/trein-video/certs/client.key \
     -X POST https://master-ip:8443/api/submit \
     -H "Content-Type: application/json" \
     -d '{
       "file_path": "/public/videos/myvideo.mkv",
       "codec": "av1",
       "should_convert": true
     }'
```

## Troubleshooting

### "No workers discovered"
- Check mDNS is enabled on all nodes (`discovery.enabled = true`)
- Ensure workers are on same subnet as master (multicast must work)
- Alternative: Set explicit `master_url` in worker config

### "Database is locked"
- SQLite WAL mode is enabled by default (see `#10`)
- If you still hit locks, reduce `max_parallel_jobs` per worker
- Ensure `~/.cache/trein-video/` is on a local filesystem (not NFS)

### "ffmpeg not found"
- Install ffmpeg on the worker machine (see Requirements)
- Verify it's in `PATH`: `which ffmpeg` should return a path

### "SMB auth failed"
- Check NAS credentials (username/password) are correct
- Ensure `password_env` var is set if using `password_env` config
- Test manually: `smbclient -U user -p pass //192.168.1.100/videos`

### Logs are verbose/empty
- Set `RUST_LOG`: `export RUST_LOG=trein_video=info,warn`
- Check stderr for panics (shouldn't be any)
- Use `LOG_FORMAT=json` for structured JSON output

## Development

### Test Suite

```sh
# Unit tests only (fast, no external tools)
cargo test --lib

# All tests including integration (requires ffmpeg/smbclient)
cargo test

# Specific test
cargo test test_claim_next_pending

# Run ffmpeg-dependent tests (ignored by default)
cargo test -- --ignored

# With logging
RUST_LOG=debug cargo test -- --nocapture
```

### Code Quality

Before committing:

```sh
cargo fmt          # Auto-format code
cargo clippy       # Lint warnings
cargo test         # Run test suite
```

### Architecture

- **Phase 1**: Rust scaffolding, CLI, config parsing, database setup
- **Phase 2**: NAS client, video analyzer, core data models
- **Phase 3**:
  - Vague 1: Converter (ffmpeg), job queue (atomic), HTTP server (mTLS), mDNS discovery, logging
  - Vague 2: Orchestration, API endpoints, encoding optimization
  - Vague 3: Worker sync, retry logic, integration tests

See `.tkt/` for detailed ticket breakdown.

## Performance

Typical benchmarks (single worker, 1080p input, AV1 encoding):

| Input | Codec | Time | Output Size |
|-------|-------|------|-------------|
| 2 GB H.264 | AV1 (crf=30) | ~15 min | 800 MB |
| 4 GB H.265 | AV1 (crf=28) | ~20 min | 1.2 GB |

With 4 parallel workers, throughput scales to ~4x.

## License

[Add your license here]

## Contributing

Contributions welcome! Please:
1. Run `cargo fmt && cargo clippy && cargo test` before pushing
2. Add tests for new features
3. Update config.example.toml if adding new fields

## Support

- **Issues**: [GitHub Issues]
- **Docs**: [Full documentation](./docs/)
- **Examples**: See `config.example.toml` and `tests/`
