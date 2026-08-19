namespace Velduct.Web.Configuration;

public class TransferOptions
{
    public const string SectionName = "TransferOptions";

    public StorageOptions Storage { get; set; } = new();
    public NetworkOptions Network { get; set; } = new();
    public DiskOptions Disk { get; set; } = new();
    public SyncOptions Sync { get; set; } = new();
    public BroadcastOptions Broadcast { get; set; } = new();
    public ArchiveOptions Archive { get; set; } = new();
    public WebSocketOptions WebSocket { get; set; } = new();
}

public class StorageOptions
{
    public string DataDirectory { get; set; } = "/app/data";

    public string TempDirectory { get; set; } = "/app/temp";
}

public class NetworkOptions
{
    // Maximum concurrent in-flight chunks per connection; affects per-connection buffer count
    public int MaxCredits { get; set; } = 12;

    // Size of global receive buffer pool (client → server); must exceed MaxCredits to prevent deadlock
    public int NetworkPoolSize { get; set; } = 16;

    public int ChunkSizeBytes { get; set; } = 4 * 1024 * 1024;

    public int HeaderBufferSize { get; set; } = 64 * 1024;

    public int MessagePayloadBufferSize { get; set; } = 256 * 1024;

    public int SendChannelCapacity { get; set; } = 4096;

    public int CreditTimeoutSeconds { get; set; } = 60;

    public int FileLockRetryDelayMs { get; set; } = 500;
}

public class DiskOptions
{
    // For HDD: must be 1 to prevent head thrashing. For SSD: 2-16 for utilization
    public int MaxConcurrentDiskIoTasks { get; set; } = 8;

    public int IoBufferSize { get; set; } = 4 * 1024 * 1024;

    public int MoveRetryDelayMs { get; set; } = 500;

    public int TarReadBufferSize { get; set; } = 2 * 1024 * 1024;

    // Per-session capacity; provides backpressure to disk subsystem
    public int CreateTasksQueueCapacity { get; set; } = 100;

    public int FinalizeTasksQueueCapacity { get; set; } = 100;
}

public class SyncOptions
{
    public int ScanBatchSize { get; set; } = 100;

    public int DeleteRetryDelayMs { get; set; } = 500;

    public int DeleteTimestampCleanupThreshold { get; set; } = 1000;

    public int DeleteTimestampTtlMinutes { get; set; } = 5;

    /// <summary>
    /// Max total time (ms) to wait for pull requests before sending a TAR to client.
    /// Matches client-side debounce; higher = bigger TARs, fewer per-archive overheads.
    /// </summary>
    public int PullCoalescingMaxMs { get; set; } = 2500;

    /// <summary>
    /// Idle gap (ms): if no new pull requests arrive within this window, flush immediately.
    /// Lower = more responsive for small syncs; higher = better batching for large syncs.
    /// </summary>
    public int PullCoalescingIdleMs { get; set; } = 250;
}

public class BroadcastOptions
{
    public int FlushIntervalMs { get; set; } = 200;
}

public class ArchiveOptions
{
    public int UnpackTaskTimeoutMs { get; set; } = 15_000;
}

public class WebSocketOptions
{
    public int KeepAliveSeconds { get; set; } = 15;

    // Reap a connection after this long with no inbound activity (dead-client
    // detection). Clients keep it fresh with a periodic app-level heartbeat, so
    // this should be a few times the client heartbeat interval. 0 disables reaping.
    public int ClientTimeoutSeconds { get; set; } = 45;

    public int SendRetryDelayMs { get; set; } = 100;

    public int KestrelBufferOverhead { get; set; } = 4096;
}
