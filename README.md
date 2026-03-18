# Velduct — Bidirectional File Sync

Velduct synchronizes files between a central server and multiple clients in real time. Changes on any client propagate to the server and from there to all other connected clients.

**Server** — ASP.NET Core 8 (.NET 8), runs in Docker
**Client** — Go 1.25, runs on Linux / Windows / macOS

---

## How it works

```
Client A ──upload──▶ Server ──broadcast──▶ Client B
Client B ──upload──▶ Server ──broadcast──▶ Client A
```

- Transport: **WebSocket** with a credit-based backpressure pipeline
- Transfer format: streaming **TAR** (no temp files on the wire)
- Auth: one-time **JWT HS256** tokens (replay-protected)
- Atomicity: every file goes through `temp → atomic rename` on both sides
- Conflict resolution: last-writer-wins by **server-authoritative mtime**

---

## Repository structure

```
Velduct/
├── Velduct.Web/          # ASP.NET Core server
│   └── Dockerfile
├── Velduct.GoClient/     # Go client
│   └── main.go
├── docker-compose.web.yml
├── docker-compose.clients.yml
├── Velduct.sln
└── LICENSE
```

---

## Quick start

### 1. Server

```bash
git clone https://github.com/kot9pa-dev/Velduct.git
cd Velduct

# Edit docker-compose.web.yml: set JwtSettings__ServerKeys__0
docker compose -f docker-compose.web.yml up -d --build

# View logs
docker compose -f docker-compose.web.yml logs -f
```

After startup the server is available at `http://host:5050` (WebSocket: `ws://host:5050/ws`).

### 2. Client

```bash
cd Velduct.GoClient

# Build for current platform
go build -o velduct-client .

# Cross-compile
GOOS=windows GOARCH=amd64 go build -o velduct-client.exe .
GOOS=linux   GOARCH=amd64 go build -o velduct-client-linux .

# Edit config.json: set server_url, jwt_key, shares
./velduct-client
```

---

## Server configuration

All parameters are set via environment variables in `docker-compose.web.yml`.

### Example docker-compose.web.yml

```yaml
services:
  web:
    container_name: velduct-web
    build:
      context: .
      dockerfile: Velduct.Web/Dockerfile
    ports:
      - "5050:8080"
    volumes:
      - ./docker-data/web/data:/app/data
      - ./docker-data/web/temp:/app/temp
      - ./docker-data/web/logs:/app/logs
    environment:
      - ASPNETCORE_ENVIRONMENT=Production

      # Paths (must be on the same volume — required!)
      - TransferOptions__Storage__DataDirectory=/app/data
      - TransferOptions__Storage__TempDirectory=/app/temp

      # Network
      - TransferOptions__Network__MaxCredits=4
      - TransferOptions__Network__ChunkSizeBytes=4194304

      # JWT — set your own keys (minimum 32 characters)
      - JwtSettings__Audience=VelductServer
      - JwtSettings__ServerKeys__0=your-secret-key-for-client-one
      # - JwtSettings__ServerKeys__1=your-secret-key-for-client-two

    restart: unless-stopped
```

> `DataDirectory` and `TempDirectory` **must** be mounted into the same Docker volume — otherwise `File.Move` will copy instead of atomically renaming.

### Configuration hierarchy

Values are loaded in ascending priority order:

1. Defaults in code (`TransferOptions.cs`)
2. `appsettings.json`
3. `appsettings.{ASPNETCORE_ENVIRONMENT}.json`
4. **Environment variables** — highest priority

Double underscore is used as section separator:

```bash
TransferOptions__Network__MaxCredits=8
TransferOptions__Storage__DataDirectory=/mnt/ssd/data
JwtSettings__ServerKeys__0=key-for-client-one
JwtSettings__ServerKeys__1=key-for-client-two
```

### Server parameter reference

#### Storage

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `DataDirectory` | string | `/app/data` | File storage directory |
| `TempDirectory` | string | `/app/temp` | Temp directory. **Must be on the same volume** as `DataDirectory` (atomic `File.Move`) |

#### Network

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `MaxCredits` | int | `4` | Pipeline depth — chunks in-flight per connection. Throughput ≈ MaxCredits × ChunkSize / RTT. **Minimum 3** |
| `NetworkPoolSize` | int | `8` | Global receive buffer pool size. Must be > MaxCredits (recommended: MaxCredits + 4) |
| `ChunkSizeBytes` | int | `4194304` | Chunk size (4 MB). Must match `chunk_size_bytes` on all clients |
| `HeaderBufferSize` | int | `65536` | WebSocket frame header buffer (64 KB) |
| `MessagePayloadBufferSize` | int | `262144` | Non-archive message buffer (256 KB) |
| `SendChannelCapacity` | int | `4096` | Unused (outgoing queue is unbounded). Kept for backward compatibility |
| `CreditTimeoutSeconds` | int | `60` | Credit timeout from client during server TAR send |
| `FileLockRetryDelayMs` | int | `500` | Retry delay on locked file |

#### Disk

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `MaxConcurrentDiskIoTasks` | int | `4` | Parallel write threads. **SSD**: 4–16. **HDD**: strictly 1 |
| `IoBufferSize` | int | `1048576` | FileStream I/O buffer (1 MB) |
| `MoveRetryDelayMs` | int | `500` | Retry delay on locked `File.Move` |
| `TarReadBufferSize` | int | `2097152` | BufferedStream for TAR extraction (2 MB) |
| `CreateTasksQueueCapacity` | int | `100` | Write queue capacity per session |
| `FinalizeTasksQueueCapacity` | int | `100` | Finalization queue capacity per session |

#### Sync

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `ScanBatchSize` | int | `100` | Files per CmdCheckFiles / CmdBatchPull batch |
| `DeleteRetryDelayMs` | int | `500` | Retry delay on locked file delete |
| `DeleteTimestampCleanupThreshold` | int | `1000` | Threshold for stale delete-record cleanup |
| `DeleteTimestampTtlMinutes` | int | `5` | Delete record TTL (phantom upload protection) |
| `PullCoalescingMaxMs` | int | `2500` | Max wait time before flushing pull requests as a TAR. Match client debounce |
| `PullCoalescingIdleMs` | int | `250` | Idle gap — if no new pull requests within this window, flush immediately |

**Derived limits** (not configurable, computed from existing parameters — same as client-side `sender.go` batch worker):
- Max files per TAR = `ScanBatchSize × MaxCredits` (default: 100 × 4 = 400)
- Max bytes per TAR = `MaxCredits × ChunkSizeBytes` (default: 4 × 4MB = 16MB)

Coalescing stops early when either limit is reached. On client side the same formula applies with client's own `max_credits` and `chunk_size_bytes`.

#### Broadcast

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `FlushIntervalMs` | int | `200` | Batch notification interval to other clients |

#### WebSocket

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `KeepAliveSeconds` | int | `15` | Keepalive ping interval |
| `SendRetryDelayMs` | int | `100` | Delay before retry on WebSocketException |
| `KestrelBufferOverhead` | int | `4096` | Kestrel MaxResponseBufferSize headroom above ChunkSizeBytes |

#### JwtSettings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `ServerKeys` | string[] | — | Client HMAC keys (HS256). Minimum 32 characters. **Required** |
| `Audience` | string | `VelductServer` | Expected `aud` claim. Must match `jwt_audience` on all clients |
| `ClockSkewMinutes` | int | `1` | Allowed clock difference between server and client |
| `UsedTokenStoreMinutes` | int | `6` | JTI retention time (replay protection). Must be ≥ client token TTL |

### Server memory formula

```
Global pool     = NetworkPoolSize × ChunkSizeBytes
Per-connection  = MaxCredits × (ChunkSizeBytes + 1)
Total (N clients) = Global pool + N × Per-connection
```

Example (MaxCredits=4, ChunkSize=4 MB, Pool=8, 10 clients):

```
Global pool    =  8 × 4 MB = 32 MB
Per-connection =  4 × 4 MB = 16 MB  × 10 clients = 160 MB
Total          ≈ 192 MB network buffers
```

### Server configuration profiles

**Slow network (< 100 Mbps, high RTT)**
```bash
TransferOptions__Network__ChunkSizeBytes=1048576
TransferOptions__Network__MaxCredits=6
```

**Fast LAN / localhost**
```bash
TransferOptions__Network__ChunkSizeBytes=4194304
TransferOptions__Network__MaxCredits=4
```

**Limited RAM (< 512 MB)**
```bash
TransferOptions__Network__ChunkSizeBytes=1048576
TransferOptions__Network__MaxCredits=3
TransferOptions__Network__NetworkPoolSize=6
TransferOptions__Disk__IoBufferSize=262144
```

**HDD (not SSD)**
```bash
TransferOptions__Disk__MaxConcurrentDiskIoTasks=1
TransferOptions__Disk__IoBufferSize=4194304
```

**Many clients (> 50)**
```bash
TransferOptions__Network__MaxCredits=3
TransferOptions__Network__NetworkPoolSize=8
TransferOptions__Broadcast__FlushIntervalMs=500
```

---

## Client configuration

### Running the client

```bash
# Basic run (reads config.json from current directory)
./velduct-client

# Explicit config file
./velduct-client -config config.dev.json
./velduct-client -config /etc/velduct/prod.json

# Via environment variable (convenient for docker/systemd)
CONFIG_PATH=config.dev.json ./velduct-client

# Full override via env without config file
JWT_KEY=your-secret-key-minimum-32-characters \
SERVER_URL=ws://192.168.1.10:5050/ws \
./velduct-client
```

Priority: **env vars** > **config file** > **code defaults**

### config.json template

```json
{
  "server_url": "ws://127.0.0.1:5050/ws",
  "shares": {
    "MyShare": "/path/to/local/directory"
  },
  "target_shares": [],
  "temp_dirs": {},

  "reconnect_interval_sec": 5,
  "max_credits": 4,
  "chunk_size_bytes": 4194304,
  "read_buffer_size": 65536,
  "write_buffer_overhead": 4096,
  "credit_timeout_sec": 60,
  "single_upload_timeout_sec": 30,

  "scan_batch_size": 100,
  "upload_queue_size": 500,
  "debounce_duration_ms": 2500,
  "startup_sync_delay_ms": 500,
  "batch_coalescing_max_ms": 2500,
  "batch_coalescing_idle_ms": 250,

  "max_copy_retries": 10,
  "file_lock_retry_delay_ms": 500,

  "watcher_event_buffer_size": 512,
  "pprof_address": "localhost:6060",

  "jwt_key": "YOUR_SECRET_KEY_MIN_32_CHARS_HERE",
  "jwt_issuer": "velduct-client",
  "jwt_audience": "VelductServer",
  "jwt_ttl_seconds": 300
}
```

### Client parameter reference

All config.json keys are mirrored by environment variables. Env always takes highest priority.

#### Server and reconnect

| Env | JSON key | Type | Default | Description |
|-----|----------|------|---------|-------------|
| `SERVER_URL` | `server_url` | string | `ws://127.0.0.1:5050/ws` | WebSocket server URL |
| `RECONNECT_INTERVAL` | `reconnect_interval_sec` | int | `5` | Reconnect delay (sec) |

#### Network performance

| Env | JSON key | Type | Default | Description |
|-----|----------|------|---------|-------------|
| `MAX_CREDITS` | `max_credits` | int | `12` | Pipeline depth — chunks in-flight. **Minimum 3** |
| `CHUNK_SIZE_BYTES` | `chunk_size_bytes` | int | `4194304` | Chunk size (4 MB). Must match server |
| `READ_BUFFER_SIZE` | `read_buffer_size` | int | `65536` | WebSocket read buffer (bytes) |
| `WRITE_BUFFER_OVERHEAD` | `write_buffer_overhead` | int | `4096` | Write buffer headroom above ChunkSize (bytes) |
| `CREDIT_TIMEOUT_SEC` | `credit_timeout_sec` | int | `60` | Credit wait timeout in TAR stream (sec) |
| `SINGLE_UPLOAD_TIMEOUT_SEC` | `single_upload_timeout_sec` | int | `30` | Credit timeout for single file upload (sec) |

#### Sync and watcher

| Env | JSON key | Type | Default | Description |
|-----|----------|------|---------|-------------|
| `SCAN_BATCH_SIZE` | `scan_batch_size` | int | `100` | Files per scan batch |
| `UPLOAD_QUEUE_SIZE` | `upload_queue_size` | int | `500` | Single-upload queue capacity |
| `DEBOUNCE_DURATION_MS` | `debounce_duration_ms` | int | `2500` | FS event debounce before sending to server (ms) |
| `STARTUP_SYNC_DELAY_MS` | `startup_sync_delay_ms` | int | `500` | Delay before initial scan after connect (ms) |
| `BATCH_COALESCING_MAX_MS` | `batch_coalescing_max_ms` | int | `2500` | Max wait time to accumulate batch before TAR send |
| `BATCH_COALESCING_IDLE_MS` | `batch_coalescing_idle_ms` | int | `250` | Idle gap — no new batches within this → flush |
| `WATCHER_EVENT_BUFFER_SIZE` | `watcher_event_buffer_size` | int | `512` | FS event channel buffer |

#### Disk I/O

| Env | JSON key | Type | Default | Description |
|-----|----------|------|---------|-------------|
| `MAX_COPY_RETRIES` | `max_copy_retries` | int | `10` | Max retries if file changes during copy |
| `FILE_LOCK_RETRY_DELAY_MS` | `file_lock_retry_delay_ms` | int | `500` | Retry delay on locked file (ms) |

#### Debug

| Env | JSON key | Type | Default | Description |
|-----|----------|------|---------|-------------|
| `PPROF_ADDRESS` | `pprof_address` | string | `localhost:6060` | pprof server address (empty to disable) |

```bash
go tool pprof http://localhost:6060/debug/pprof/heap
go tool pprof http://localhost:6060/debug/pprof/goroutine
```

#### JWT authentication

| Env | JSON key | Type | Default | Description |
|-----|----------|------|---------|-------------|
| `JWT_KEY` | `jwt_key` | string | — | HMAC key (HS256), minimum 32 characters. **Required** |
| `JWT_ISSUER` | `jwt_issuer` | string | `velduct-client` | `iss` claim — unique client name |
| `JWT_AUDIENCE` | `jwt_audience` | string | `VelductServer` | `aud` claim. Must match server |
| `JWT_TTL_SECONDS` | `jwt_ttl_seconds` | int | `300` | Token TTL (sec). Must be < `UsedTokenStoreMinutes × 60` on server |

### shares and target_shares

`shares` — directories the client syncs with the server in both directions:

```json
{
  "shares": {
    "documents": "/home/user/Documents",
    "photos":    "/mnt/nas/Photos"
  }
}
```

`target_shares` — share keys the client **only receives** (download-only). The client does not scan them on startup and does not send watcher events for them:

```json
{
  "target_shares": ["photos"]
}
```

### temp_dirs

Temporary directories for atomic operations. By default created as `{share_path}/.ddzs_temp`.

**Requirements:**
- Must be on the **same filesystem** as the share (otherwise `os.Rename` falls back to slow copy)
- **Must not overlap** with any share path (fatal error on startup)

```json
{
  "shares": {
    "docs": "/mnt/nas/docs"
  },
  "temp_dirs": {
    "docs": "/mnt/nas/.ddzs_temp_docs"
  }
}
```

### Client memory formula

```
Pools = MaxCredits × ChunkSizeBytes × 3
        (uploadBufPool + tarBufPool + tarCopyPool)

WebSocket write buffer = ChunkSizeBytes + WriteBufferOverhead
WebSocket read buffer  = ReadBufferSize
```

Example (MaxCredits=4, ChunkSize=4 MB):

```
Pools     = 4 × 4 MB × 3 = 48 MB
WebSocket ≈ 4 MB
Total     ≈ 52 MB
```

### Client configuration profiles

**Production (SSD, fast network)**
```json
{
  "max_credits": 8,
  "chunk_size_bytes": 4194304,
  "debounce_duration_ms": 2500,
  "upload_queue_size": 500
}
```

**Slow network (high RTT, < 100 Mbps)**
```json
{
  "max_credits": 12,
  "chunk_size_bytes": 1048576,
  "credit_timeout_sec": 120,
  "single_upload_timeout_sec": 60,
  "debounce_duration_ms": 5000
}
```

**Limited RAM (< 256 MB)**
```json
{
  "max_credits": 3,
  "chunk_size_bytes": 1048576,
  "read_buffer_size": 32768,
  "upload_queue_size": 100,
  "watcher_event_buffer_size": 128
}
```

**HDD (not SSD)**
```json
{
  "max_copy_retries": 20,
  "file_lock_retry_delay_ms": 1000,
  "debounce_duration_ms": 5000,
  "scan_batch_size": 50
}
```

### Systemd (Linux autostart)

```ini
# /etc/systemd/system/velduct-client.service
[Unit]
Description=Velduct Client
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/velduct/client1
ExecStart=/opt/velduct/client1/velduct-client
Restart=always
RestartSec=5
Environment=JWT_KEY=your-secret-key-minimum-32-characters

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable velduct-client
sudo systemctl start velduct-client
sudo journalctl -u velduct-client -f
```

### Docker (client)

```dockerfile
FROM golang:1.25 AS builder
WORKDIR /src
COPY . .
RUN go build -o velduct-client .

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /src/velduct-client .

ENV SERVER_URL=ws://server:5050/ws
ENV JWT_KEY=your-secret-key-minimum-32-characters

ENTRYPOINT ["./velduct-client"]
```

```bash
docker build -t velduct-client .
docker run -d \
  -e SERVER_URL=ws://192.168.1.10:5050/ws \
  -e JWT_KEY=your-secret-key-minimum-32-characters \
  -e MAX_CREDITS=4 \
  -e CHUNK_SIZE_BYTES=4194304 \
  -v /host/share:/data/share \
  velduct-client
```

---

## Server-authoritative mtime

The server is the single source of truth for file timestamps. Client clocks are not used for conflict resolution.

```
Client uploads file.txt (local mtime = 13:23, client clock)
  → Server receives, stamps DateTime.UtcNow = 13:25:00.123
  → Server writes to disk with mtime = 13:25:00.123
  → Server → uploading client: CMD_FILE_MTIME_ACK (key, path, 13:25:00.123)
  → Server → other clients: broadcast CHECK_FILES (key, path, 13:25:00.123)
  → Uploading client: os.Chtimes(file.txt, 13:25:00.123)
  → Other clients: download, os.Chtimes(file.txt, 13:25:00.123)
  → Result: all systems have mtime = 13:25:00.123
```

**FS precision detection:**
- At startup, the Go client probes each share directory by writing test timestamps and reading them back (package `internal/fsinfo`). Detected values: FAT32=2000ms, HFS+=1000ms, NTFS/APFS/ext4=1ms. Worst case across all shares is used
- The server reads back actual mtime after `File.SetLastWriteTimeUtc` — cache, ACK, and broadcast all use the FS-normalized value (not the requested value)
- Client-side comparisons truncate both mtimes to detected precision before comparing — no hardcoded epsilon
- Server uses 2-second tolerance for mtime comparison in `ClassifyFileForPull` — covers cross-FS rounding when clients report FS-rounded mtime values
- In Docker, the probe runs on the actual mounted volume — detects the real FS precision regardless of host OS

**Same FS on both sides → exact comparison, zero tolerance:**

When client and server share the same filesystem type (e.g., both ext4, or both NTFS), precision=1ms on both sides. Truncation is identity (`truncateMs(x, 1) = x`). Comparison is effectively exact `==`. No rounding, no lost precision — the mtime travels through the entire pipeline bit-for-bit.

**Cross-FS example (server ext4, client FAT32):**

Server writes `1710000000123` → read-back = `1710000000123` (ext4 preserves ms). Client receives, Chtimes → FAT32 rounds to `1710000000000`. On next comparison: `truncateMs(1710000000123, 2000) = 1710000000000` = `truncateMs(1710000000000, 2000)` → match, no unnecessary transfer.

**Clock skew handling:**
- When a client edits a previously-synced file and its clock is behind the server, `ResolveReportMtime` bumps the reported mtime to `serverMtime + fsPrecisionMs + 1` to guarantee the server pulls the updated version
- On NTFS/ext4 (precision=1ms) bump is +2ms; on FAT32 (precision=2000ms) bump is +2001ms — adapts automatically to ensure the bumped value lands in a different truncation bucket

**Anti-echo and ACK pre-store:**
- When the server sends a mtime ACK, the client pre-stores the server mtime in `recentDownloads` immediately from the read loop (before the ACK worker runs `Chtimes`). This prevents watcher race conditions: even if the watcher fires before `Chtimes` completes, `ResolveReportMtime` already returns the correct server mtime
- The ACK worker queue is unbounded (mutex + slice) — never drops ACKs, never blocks the read loop

**Credit timeout behavior:**
- If the server does not receive credits from a client within `CreditTimeoutSeconds`, the TAR send is aborted and the WebSocket is closed. The client reconnects and re-syncs missing files
- On the client side, credit timeout during upload cancels the session. The reconnect loop picks up automatically

**Known limitations:**
- After client restart, `recentDownloads` is empty. Server-side 2-second tolerance covers FS rounding, but clock skew >2s may cause one extra round-trip per file on first sync. After mtime ACKs are processed, mtimes stabilize
- If FS probe fails (read-only share, network mount error), falls back to 2000ms (FAT32 worst case). Check logs for `"Detected mtime precision"` to verify
- Same size + same mtime (after truncation) + different content = undetectable without content hashing. This is inherent to mtime-based sync

**Protocol:** opcode `CMD_FILE_MTIME_ACK` (`0x17`) — sent by server to the uploading client after successful disk write. Format: `[opcode][4-byte keyLen][key][4-byte pathLen][path][8-byte serverMtimeMs]`

---

## Running tests

### Server (C# / xUnit)

```bash
dotnet test Velduct.Web.Tests/
dotnet test Velduct.Web.Tests/ -v detailed
dotnet test Velduct.Web.Tests/ --filter "ProtocolTests"
dotnet test Velduct.Web.Tests/ --filter "MtimeIntegrationTests"
```

### Client (Go)

```bash
# bash / sh / zsh
cd Velduct.GoClient && go test ./internal/transfer/ ./internal/sync/ -v

# PowerShell (does not support &&)
cd Velduct.GoClient; go test ./internal/transfer/ ./internal/sync/ -v; cd ..
```

---

## Critical constraints

- `MaxCredits` / `max_credits` minimum **3** — lower values can deadlock the TAR uploader (double buffer pool acquisition)
- `NetworkPoolSize` must be strictly greater than `MaxCredits` — otherwise the pool empties and the server stops issuing credits
- `ChunkSizeBytes` must match `chunk_size_bytes` on every client
- `JwtSettings.Audience` must match `jwt_audience` on every client
- `jwt_ttl_seconds` must be less than `JwtSettings.UsedTokenStoreMinutes × 60` on the server
- `DataDirectory` and `TempDirectory` must be on the same filesystem (same partition or volume)

## Security

- **Path traversal protection**: all incoming file paths (TAR entries, file offers, mtime ACKs, sync requests) are validated with `GetFullPath` + prefix check. Paths that escape the share directory are rejected and logged
- **Input bounds validation**: protocol parsers validate `count`, `keyLen`, `pathLen` for negative values and buffer overflow before allocation
- **Graceful shutdown**: Go client handles SIGTERM/SIGINT — closes current session cleanly, stops watcher, exits reconnect loop

---

## License

MIT — Copyright (c) 2025 KoT9pA
