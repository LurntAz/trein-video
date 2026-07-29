# 🚀 Trein Video Worker Deployment Guide

A complete guide to deploying and scaling workers for the Trein Video converter cluster.

## 1️⃣ Where to Run Your Worker?

Choose the deployment option that fits your infrastructure:

### 🏠 Local (Same Machine as Master)

**Best for**: Development, testing, single-machine setups

**Pros:**
- Simple setup, no network configuration
- Share network stack with master
- Easy debugging and log access

**Cons:**
- Shared CPU/RAM with master
- No horizontal scaling
- Performance degradation if overloaded

**Quick Start:**
```bash
# Terminal 1: Run master
export NAS_PASSWORD="your-password"
./target/release/trein-video --config ~/.config/trein-video-master.toml

# Terminal 2: Run worker (same machine)
export NAS_PASSWORD="your-password"
./target/release/trein-video --config ~/.config/trein-video-worker.toml
```

---

### 🖥️ Remote Machine (LAN)

**Best for**: Production scaling, leveraging multiple machines

**Pros:**
- Dedicated CPU/RAM per worker
- Horizontal scaling (add more machines)
- Better throughput and reliability

**Cons:**
- Network setup required (firewall, DNS)
- Potential latency for NAS access
- More complex troubleshooting

**Setup:**
1. Prepare remote machine (see Prerequisites section)
2. Copy config and build/binaries to remote
3. Run worker connecting to master via explicit URL

```bash
# On remote machine
export NAS_PASSWORD="your-password"
./trein-video-worker --config ~/.config/trein-video/config.toml
```

---

### 🐳 Docker/Container

**Best for**: Easy deployment, CI/CD pipelines, standardized environments

**Pros:**
- Isolated environment, reproducible builds
- Easy scaling with orchestration (Kubernetes, Docker Compose)
- Automatic updates via image rebuilds
- Resource limits and monitoring built-in

**Cons:**
- Slight CPU/RAM overhead vs bare metal
- Docker daemon must be running
- Requires maintaining a Dockerfile

**Quick Start:**
```bash
# Build image
docker build -t trein-video:latest .

# Run worker
docker run \
  --name trein-worker-1 \
  -v ~/.config/trein-video:/config \
  -e NAS_PASSWORD="your-password" \
  trein-video:latest \
  worker --config /config/config.toml
```

**Docker Compose (multiple workers):**
```yaml
version: '3.9'

services:
  worker-1:
    image: trein-video:latest
    environment:
      NAS_PASSWORD: your-password
      RUST_LOG: trein_video=info
    volumes:
      - ~/.config/trein-video:/config
    command: worker --config /config/worker-1.toml

  worker-2:
    image: trein-video:latest
    environment:
      NAS_PASSWORD: your-password
      RUST_LOG: trein_video=info
    volumes:
      - ~/.config/trein-video:/config
    command: worker --config /config/worker-2.toml
```

---

### ☁️ Cloud (AWS/GCP/Azure)

**Best for**: Massive scaling, managed infrastructure, pay-as-you-go

**Pros:**
- Auto-scaling based on queue depth
- Global geographic distribution
- Managed networking, monitoring, logging
- Pay only for what you use

**Cons:**
- Highest cost and complexity
- Network latency to NAS (consider running NAS in same region)
- Requires cloud provider knowledge

**Example (AWS EC2):**
```bash
# Launch t3.large EC2 instances with Ubuntu 22.04 AMI
# Security group: Allow port 8443 (HTTPS), 8444 (API) from master

# On EC2 instance:
sudo apt-get update && sudo apt-get install -y ffmpeg smbclient curl

# Download binary
curl -L https://github.com/LurntAz/trein-video/releases/download/v0.1.0/trein-video-linux-x86_64 \
  -o trein-video
chmod +x trein-video

# Copy config and run
./trein-video --config ~/.config/trein-video/config.toml
```

---

## 2️⃣ Prerequisites

### System Dependencies

All workers need:

```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y ffmpeg smbclient libssl-dev build-essential

# macOS (Homebrew)
brew install ffmpeg samba openssl

# Fedora/RHEL
sudo dnf install ffmpeg samba-client openssl-devel

# Alpine (for Docker)
apk add ffmpeg samba-client openssl
```

### Rust (if building from source)

```bash
# Install rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# Verify
rustc --version  # Should be 1.70+
```

### Network Requirements

| Component | Port | Protocol | Direction |
|-----------|------|----------|-----------|
| Master API | 8443 | HTTPS (mTLS) | Worker → Master |
| Worker API | 8444 | HTTPS (mTLS) | Master → Worker (optional) |
| mDNS Discovery | 5353 | UDP | Worker ← → Master |
| NAS (SMB) | 445 | TCP | Worker → NAS |

### Hardware Recommendations

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| CPU Cores | 2 | 4+ (8+ for production) |
| RAM | 2 GB | 8 GB+ |
| Disk | 10 GB (system) | 50+ GB (scratch space for conversions) |
| Network | 1 Gbps | 10 Gbps (for video-heavy workloads) |

---

## 3️⃣ Configuring Your Worker

### Creating the Configuration File

Copy the example config and modify for your worker:

```bash
mkdir -p ~/.config/trein-video
cp config.example.toml ~/.config/trein-video/config.toml
```

### Key Differences: Worker vs Master

```toml
[instance]
# WORKER: Unique ID per worker
id = "worker-gpu-01"
# WORKER: Always "worker" (master is "master")
role = "worker"
# WORKER: Each worker can use different port (for multiple workers)
api_port = 8444

[nas]
# IDENTICAL to master
protocol = "smb"
host = "192.168.1.100"
share = "videos"
username = "user"
password_env = "NAS_PASSWORD"
base_path = "/videos"

[conversion]
# WORKER: Often use faster presets (master uses higher quality)
codec = "av1"
preset = "fast"      # fast, medium, or slow
crf = 32             # same as master
max_parallel_jobs = 2

[sync]
poll_interval_secs = 30
# WORKER: Explicitly set master_url if mDNS not available
master_url = "https://192.168.1.10:8443"

[db]
# WORKER: Separate database per worker (for state tracking)
path = "~/.cache/trein-video/worker-gpu-01.db"

[tls]
# WORKER: Use client certificates (reuse from master)
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

### Example: Multi-Worker Configuration

For multiple workers on the same machine or LAN:

**~/.config/trein-video/worker-1.toml:**
```toml
[instance]
id = "worker-cpu-01"
role = "worker"
api_port = 8444

[nas]
host = "192.168.1.100"
share = "videos"
username = "user"
password_env = "NAS_PASSWORD"
base_path = "/videos"

[conversion]
codec = "av1"
preset = "medium"
crf = 30
max_parallel_jobs = 4

[sync]
poll_interval_secs = 30
master_url = "https://192.168.1.10:8443"

[db]
path = "~/.cache/trein-video/worker-cpu-01.db"

[tls]
cert_path = "~/.config/trein-video/certs/client.crt"
key_path = "~/.config/trein-video/certs/client.key"
ca_cert_path = "~/.config/trein-video/certs/ca.crt"
```

**~/.config/trein-video/worker-2.toml:** (identical except `id` and `db.path`)

---

## 4️⃣ Launching Your Worker

### Option 1: Build from Source

```bash
# Clone and build
git clone https://github.com/LurntAz/trein-video.git
cd trein-video
cargo build --release

# Set credentials
export NAS_PASSWORD="your-nas-password"
export RUST_LOG="trein_video=info"

# Launch worker
./target/release/trein-video \
  --config ~/.config/trein-video/config.toml
```

### Option 2: Pre-Compiled Binary

```bash
# Download from GitHub Releases
curl -L https://github.com/LurntAz/trein-video/releases/download/v0.1.0/trein-video-linux-x86_64 \
  -o ~/bin/trein-video
chmod +x ~/bin/trein-video

# Set credentials and run
export NAS_PASSWORD="your-nas-password"
export RUST_LOG="trein_video=info"

~/bin/trein-video --config ~/.config/trein-video/config.toml
```

### Option 3: Docker

```bash
# Using Docker (replace ubuntu:22.04 with your base image)
docker run -d \
  --name trein-worker-1 \
  -v ~/.config/trein-video:/config \
  -e NAS_PASSWORD="your-password" \
  -e RUST_LOG="trein_video=info" \
  trein-video:latest \
  worker --config /config/config.toml

# Check logs
docker logs -f trein-worker-1
```

### Option 4: Systemd Service (Production)

Create `/etc/systemd/system/trein-worker.service`:

```ini
[Unit]
Description=Trein Video Worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=trein-worker
WorkingDirectory=/opt/trein-video
ExecStart=/opt/trein-video/trein-video --config /etc/trein-video/config.toml
Restart=always
RestartSec=10
Environment="NAS_PASSWORD=your-password"
Environment="RUST_LOG=trein_video=info"

[Install]
WantedBy=multi-user.target
```

```bash
# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable trein-worker
sudo systemctl start trein-worker

# Check status
sudo systemctl status trein-worker
journalctl -u trein-worker -f
```

---

## 5️⃣ Complete Example: Remote Worker Setup

**Goal**: Deploy a worker on a fresh Ubuntu 22.04 machine in your LAN.

### Step 1: Prepare the Machine

```bash
# SSH into remote machine
ssh ubuntu@192.168.1.50

# Install dependencies
sudo apt-get update
sudo apt-get install -y ffmpeg smbclient libssl-dev curl

# Create user
sudo useradd -m -s /bin/bash trein-worker
sudo su - trein-worker
```

### Step 2: Copy Configuration

```bash
# On your local machine
scp ~/.config/trein-video/config.toml ubuntu@192.168.1.50:/home/ubuntu/

# SSH back and set it up
ssh ubuntu@192.168.1.50

mkdir -p ~/.config/trein-video
mv config.toml ~/.config/trein-video/

# Edit to set correct master_url
nano ~/.config/trein-video/config.toml
# Change: master_url = "https://192.168.1.10:8443"
```

### Step 3: Download Binary

```bash
# Download latest release
curl -L https://github.com/LurntAz/trein-video/releases/download/v0.1.0/trein-video-linux-x86_64 \
  -o ~/trein-video
chmod +x ~/trein-video

# Verify
./trein-video --help
```

### Step 4: Launch Worker

```bash
# Export credentials (or add to ~/.bashrc)
export NAS_PASSWORD="your-nas-password"
export RUST_LOG="trein_video=info,tokio=warn"

# Start worker
./trein-video --config ~/.config/trein-video/config.toml &
```

### Step 5: Verify It's Running

```bash
# Check logs (worker should connect to master)
# Output should show:
# INFO trein_video: Connected to master at https://192.168.1.10:8443
# INFO trein_video: Worker heartbeat sent

# Query master API to see connected workers
curl -k https://192.168.1.10:8443/api/instances \
  --cacert ~/.config/trein-video/certs/ca.crt \
  --cert ~/.config/trein-video/certs/client.crt \
  --key ~/.config/trein-video/certs/client.key | jq .
```

---

## 6️⃣ Troubleshooting

| Problem | Cause | Solution |
|---------|-------|----------|
| **Master not reachable** | Incorrect `master_url`, firewall, DNS | Check `master_url` config, test `curl` to master IP, check firewall rules |
| **Connection refused** | Master not listening on port 8443 | Verify master is running: `ps aux \| grep trein-video`, check logs |
| **Failed to connect to SMB** | Wrong NAS credentials | Test manually: `smbclient -U user -p pass //192.168.1.100/videos`, verify `NAS_PASSWORD` env var |
| **Out of disk space** | Temp files accumulating | Check: `du -sh ~/.cache/trein-video/`, clean old DBs, increase disk |
| **CPU maxed at 100%** | Too many parallel jobs | Lower `max_parallel_jobs` in config (start at 2-4) |
| **SSL certificate mismatch** | TLS paths incorrect or missing certs | Verify cert files exist: `ls -la ~/.config/trein-video/certs/` |
| **Worker connects then crashes** | Out of memory or panic | Check `free -h`, review logs with `RUST_LOG=debug` |
| **Jobs not picked up** | Master unreachable or mDNS issue | Use explicit `master_url`, test `curl` connectivity |

### Debug Mode

Enable verbose logging:

```bash
export RUST_LOG=trein_video=debug,tokio=debug
./trein-video --config ~/.config/trein-video/config.toml 2>&1 | tee worker.log
```

---

## 7️⃣ Monitoring and Logs

### Viewing Logs

```bash
# Real-time logs (stdout)
./trein-video --config config.toml

# JSON formatted logs (for log aggregation)
export LOG_FORMAT=json
./trein-video --config config.toml

# With timestamps and debug level
export RUST_LOG=trein_video=debug
./trein-video --config config.toml | tee worker.log
```

### Checking Worker Status

```bash
# Query master API for all connected workers
curl -k https://master-ip:8443/api/instances \
  --cacert ~/.config/trein-video/certs/ca.crt \
  --cert ~/.config/trein-video/certs/client.crt \
  --key ~/.config/trein-video/certs/client.key | jq .

# Example output:
# [
#   {
#     "id": "worker-gpu-01",
#     "role": "worker",
#     "status": "healthy",
#     "last_heartbeat": "2025-07-30T14:32:10Z",
#     "jobs_in_progress": 2
#   }
# ]
```

### Job Status

```bash
# List all jobs
curl -k https://master-ip:8443/api/jobs \
  --cacert ~/.config/trein-video/certs/ca.crt \
  --cert ~/.config/trein-video/certs/client.crt \
  --key ~/.config/trein-video/certs/client.key | jq .

# Get specific job
curl -k https://master-ip:8443/api/jobs/<job-id> \
  --cacert ~/.config/trein-video/certs/ca.crt \
  --cert ~/.config/trein-video/certs/client.crt \
  --key ~/.config/trein-video/certs/client.key | jq .
```

---

## 8️⃣ Horizontal Scaling

### Scaling Strategy

| # Workers | Workload | CPU Cores | RAM | Notes |
|-----------|----------|-----------|-----|-------|
| 1 | Test/Dev | 2-4 | 2-4 GB | Good for testing |
| 2-4 | Small office | 4-8 per worker | 8 GB | Covers typical video conversions |
| 4-8 | Medium workload | 8-16 per worker | 16 GB | Good parallelism |
| 8+ | Enterprise | 16+ per worker | 32+ GB | Requires load monitoring |

### Launching Multiple Workers (Same Machine)

```bash
# Terminal 1: Worker 1
export NAS_PASSWORD="your-password"
./trein-video --config ~/.config/trein-video/worker-1.toml &

# Terminal 2: Worker 2 (different config, port)
export NAS_PASSWORD="your-password"
./trein-video --config ~/.config/trein-video/worker-2.toml &

# Terminal 3: Worker 3
export NAS_PASSWORD="your-password"
./trein-video --config ~/.config/trein-video/worker-3.toml &

# Check all running
ps aux | grep trein-video
```

### Docker Compose (Multiple Workers)

```yaml
version: '3.9'

services:
  master:
    image: trein-video:latest
    environment:
      NAS_PASSWORD: ${NAS_PASSWORD}
      RUST_LOG: trein_video=info
    volumes:
      - ~/.config/trein-video:/config
    ports:
      - "8443:8443"
    command: master --config /config/master.toml

  worker-cpu-1:
    image: trein-video:latest
    depends_on:
      - master
    environment:
      NAS_PASSWORD: ${NAS_PASSWORD}
      RUST_LOG: trein_video=info
    volumes:
      - ~/.config/trein-video:/config
    command: worker --config /config/worker-1.toml

  worker-cpu-2:
    image: trein-video:latest
    depends_on:
      - master
    environment:
      NAS_PASSWORD: ${NAS_PASSWORD}
      RUST_LOG: trein_video=info
    volumes:
      - ~/.config/trein-video:/config
    command: worker --config /config/worker-2.toml

  worker-gpu-1:
    image: trein-video:gpu
    depends_on:
      - master
    runtime: nvidia
    environment:
      NAS_PASSWORD: ${NAS_PASSWORD}
      RUST_LOG: trein_video=info
    volumes:
      - ~/.config/trein-video:/config
    command: worker --config /config/worker-gpu.toml
```

### Kubernetes (Enterprise Scale)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: trein-worker
spec:
  replicas: 4
  selector:
    matchLabels:
      app: trein-worker
  template:
    metadata:
      labels:
        app: trein-worker
    spec:
      containers:
      - name: worker
        image: trein-video:latest
        env:
        - name: NAS_PASSWORD
          valueFrom:
            secretKeyRef:
              name: trein-secrets
              key: nas-password
        - name: RUST_LOG
          value: "trein_video=info"
        resources:
          requests:
            cpu: 4
            memory: 8Gi
          limits:
            cpu: 8
            memory: 16Gi
        volumeMounts:
        - name: config
          mountPath: /config
      volumes:
      - name: config
        configMap:
          name: trein-worker-config
```

### Monitoring Multiple Workers

```bash
#!/bin/bash
# Script: monitor-workers.sh

while true; do
  echo "=== Trein Video Cluster Status ==="
  curl -sk https://master-ip:8443/api/instances \
    --cert ~/.config/trein-video/certs/client.crt \
    --key ~/.config/trein-video/certs/client.key \
    --cacert ~/.config/trein-video/certs/ca.crt \
    | jq '.[] | "\(.id): \(.status) (\(.jobs_in_progress) jobs)"'
  sleep 10
done
```

---

## Next Steps

1. ✅ Choose your deployment option (local, remote, Docker, cloud)
2. ✅ Prepare infrastructure (hardware, network, NAS access)
3. ✅ Create worker configuration (`config.toml`)
4. ✅ Launch worker and verify connection
5. ✅ Start scaling with additional workers
6. ✅ Set up monitoring and alerting

**Questions?** Check the main [README.md](../README.md) or open an issue on GitHub.
