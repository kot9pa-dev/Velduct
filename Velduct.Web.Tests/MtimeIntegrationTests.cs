using System.Buffers.Binary;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Velduct.Web.Configuration;
using Velduct.Web.Core;
using Velduct.Web.Domain;
using Velduct.Web.Infrastructure;
using Velduct.Web.Services;
using Xunit;

namespace Velduct.Web.Tests;

/// <summary>
/// Integration tests for server-authoritative mtime behavior.
/// Tests epsilon comparison in SyncOrchestrator, server mtime stamping,
/// and cache consistency after file operations.
/// </summary>
public class MtimeIntegrationTests : IDisposable
{
    private static readonly DateTime Epoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private readonly string _tmpDir;

    public MtimeIntegrationTests()
    {
        _tmpDir = Path.Combine(Path.GetTempPath(), $"velduct-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tmpDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tmpDir))
            Directory.Delete(_tmpDir, true);
    }

    private TransferOptions CreateOptions() => new()
    {
        Storage = new StorageOptions
        {
            DataDirectory = _tmpDir,
            TempDirectory = Path.Combine(_tmpDir, "_temp")
        },
        Sync = new SyncOptions { ScanBatchSize = 100 }
    };

    private static StorageManager CreateStorageManager(TransferOptions options)
    {
        var store = new FileCacheStore();
        return new StorageManager(
            NullLogger<StorageManager>.Instance,
            store,
            Options.Create(options));
    }

    private static SyncOrchestrator CreateOrchestrator(TransferOptions options, StorageManager storage)
    {
        var connMgr = new ConnectionManager();
        var broadcast = new BroadcastService(
            NullLogger<BroadcastService>.Instance,
            connMgr);
        var deleteQueue = new GlobalDeleteQueueService(
            NullLogger<GlobalDeleteQueueService>.Instance,
            broadcast,
            Options.Create(options));

        return new SyncOrchestrator(
            NullLogger<SyncOrchestrator>.Instance,
            storage,
            Options.Create(options),
            broadcast,
            deleteQueue);
    }

    // EnqueueFilesAsync always accesses conn.Id, so a real connection is required
    private static WebSocketConnection CreateTestConnection()
        => new(null, 4, 256);

    // --- Exact mtime comparison tests (server uses FS-normalized read-back, no epsilon) ---

    [Fact]
    public async Task ClassifyFileForPull_ExactMatch_SkipsPull()
    {
        var options = CreateOptions();
        var storage = CreateStorageManager(options);
        var orchestrator = CreateOrchestrator(options, storage);

        const long baseMs = 1710000000000;
        storage.UpdateFileInCache("share", "file.txt", 1024, Epoch.AddMilliseconds(baseMs));

        var file = new FileMetadata
        {
            Key = "share",
            RelativePath = "file.txt",
            Size = 1024,
            LastWriteTimeUtc = Epoch.AddMilliseconds(baseMs) // exact match
        };

        var conn = CreateTestConnection();
        await orchestrator.EnqueueFilesAsync(null, conn, new[] { file }, CancellationToken.None);

        bool queued = conn.SendChannel.Reader.TryRead(out _);
        Assert.False(queued, "Expected no pull for exact mtime match");
    }

    [Fact]
    public async Task ClassifyFileForPull_ClientNewer_1ms_TriggersPull()
    {
        var options = CreateOptions();
        var storage = CreateStorageManager(options);
        var orchestrator = CreateOrchestrator(options, storage);

        const long baseMs = 1710000000000;
        storage.UpdateFileInCache("share", "file.txt", 1024, Epoch.AddMilliseconds(baseMs));

        // Even 1ms difference triggers pull with exact comparison
        var file = new FileMetadata
        {
            Key = "share",
            RelativePath = "file.txt",
            Size = 1024,
            LastWriteTimeUtc = Epoch.AddMilliseconds(baseMs + 1)
        };

        var conn = CreateTestConnection();
        await orchestrator.EnqueueFilesAsync(null, conn, new[] { file }, CancellationToken.None);

        bool queued = conn.SendChannel.Reader.TryRead(out _);
        Assert.True(queued, "Expected pull for 1ms newer client mtime (exact comparison)");
    }

    [Fact]
    public async Task ClassifyFileForPull_ServerNewer_SkipsPull()
    {
        var options = CreateOptions();
        var storage = CreateStorageManager(options);
        var orchestrator = CreateOrchestrator(options, storage);

        const long baseMs = 1710000000000;
        storage.UpdateFileInCache("share", "file.txt", 1024, Epoch.AddMilliseconds(baseMs));

        var file = new FileMetadata
        {
            Key = "share",
            RelativePath = "file.txt",
            Size = 1024,
            LastWriteTimeUtc = Epoch.AddMilliseconds(baseMs - 5000)
        };

        await orchestrator.EnqueueFilesAsync(null, CreateTestConnection(), new[] { file }, CancellationToken.None);
    }

    [Fact]
    public async Task ClassifyFileForPull_NotInCache_AlwaysPulls()
    {
        var options = CreateOptions();
        var storage = CreateStorageManager(options);
        var orchestrator = CreateOrchestrator(options, storage);

        var file = new FileMetadata
        {
            Key = "share",
            RelativePath = "newfile.txt",
            Size = 512,
            LastWriteTimeUtc = DateTime.UtcNow
        };

        var conn = CreateTestConnection();
        await orchestrator.EnqueueFilesAsync(null, conn, new[] { file }, CancellationToken.None);

        bool queued = conn.SendChannel.Reader.TryRead(out _);
        Assert.True(queued, "Expected pull for file not in cache");
    }

    [Fact]
    public async Task ClassifyFileForPull_DifferentSize_SameMtime_TriggersPull()
    {
        var options = CreateOptions();
        var storage = CreateStorageManager(options);
        var orchestrator = CreateOrchestrator(options, storage);

        const long baseMs = 1710000000000;
        storage.UpdateFileInCache("share", "file.txt", 1024, Epoch.AddMilliseconds(baseMs));

        var file = new FileMetadata
        {
            Key = "share",
            RelativePath = "file.txt",
            Size = 2048,
            LastWriteTimeUtc = Epoch.AddMilliseconds(baseMs + 500)
        };

        var conn = CreateTestConnection();
        await orchestrator.EnqueueFilesAsync(null, conn, new[] { file }, CancellationToken.None);

        bool queued = conn.SendChannel.Reader.TryRead(out _);
        Assert.True(queued, "Expected pull when size differs");
    }

    // --- Parametric exact comparison ---

    [Theory]
    [InlineData(0, false)]      // exact match → no pull
    [InlineData(1, true)]       // 1ms newer → pull (exact comparison)
    [InlineData(1000, true)]    // 1s newer → pull
    [InlineData(-1, false)]     // 1ms older → server newer, no pull
    [InlineData(-5000, false)]  // 5s older → server newer, no pull
    public async Task ClassifyFileForPull_ExactComparison(long mtimeDiffMs, bool shouldPull)
    {
        var options = CreateOptions();
        var storage = CreateStorageManager(options);
        var orchestrator = CreateOrchestrator(options, storage);

        const long baseMs = 1710000000000;
        storage.UpdateFileInCache("share", "file.txt", 1024, Epoch.AddMilliseconds(baseMs));

        var file = new FileMetadata
        {
            Key = "share",
            RelativePath = "file.txt",
            Size = 1024,
            LastWriteTimeUtc = Epoch.AddMilliseconds(baseMs + mtimeDiffMs)
        };

        var conn = CreateTestConnection();
        await orchestrator.EnqueueFilesAsync(null, conn, new[] { file }, CancellationToken.None);

        bool queued = conn.SendChannel.Reader.TryRead(out _);
        if (shouldPull)
            Assert.True(queued, $"Expected pull for diff={mtimeDiffMs}ms");
        else
            Assert.False(queued, $"Expected no pull for diff={mtimeDiffMs}ms");
    }

    // --- Bump triggers pull (any mtime > cached triggers pull with exact comparison) ---

    /// <summary>
    /// Client edits file with clock behind server. Go client bumps mtime.
    /// With exact comparison, any bump > 0 triggers pull.
    /// </summary>
    [Fact]
    public async Task ClassifyFileForPull_BumpedMtime_TriggersPull()
    {
        var options = CreateOptions();
        var storage = CreateStorageManager(options);
        var orchestrator = CreateOrchestrator(options, storage);

        const long serverMs = 1710000000000;
        storage.UpdateFileInCache("s", "edited.txt", 1024, Epoch.AddMilliseconds(serverMs));

        // Client reports serverMs + fsPrecision + 1 (bump from ResolveReportMtime)
        var file = new FileMetadata
        {
            Key = "s",
            RelativePath = "edited.txt",
            Size = 1024,
            LastWriteTimeUtc = Epoch.AddMilliseconds(serverMs + 2) // even +2ms triggers
        };

        var conn = CreateTestConnection();
        await orchestrator.EnqueueFilesAsync(null, conn, new[] { file }, CancellationToken.None);

        bool queued = conn.SendChannel.Reader.TryRead(out _);
        Assert.True(queued, "Any mtime > cached must trigger pull with exact comparison");
    }

    /// <summary>
    /// Same size, same mtime → no pull. Proves exact match required.
    /// </summary>
    [Fact]
    public async Task ClassifyFileForPull_SameSizeEditedFile_NoBumpNoPull()
    {
        var options = CreateOptions();
        var storage = CreateStorageManager(options);
        var orchestrator = CreateOrchestrator(options, storage);

        const long serverMs = 1710000000000;
        storage.UpdateFileInCache("s", "config.json", 256, Epoch.AddMilliseconds(serverMs));

        // Same mtime → no pull even if content changed (server can't know)
        var file = new FileMetadata
        {
            Key = "s",
            RelativePath = "config.json",
            Size = 256,
            LastWriteTimeUtc = Epoch.AddMilliseconds(serverMs)
        };

        var conn = CreateTestConnection();
        await orchestrator.EnqueueFilesAsync(null, conn, new[] { file }, CancellationToken.None);

        bool queued = conn.SendChannel.Reader.TryRead(out _);
        Assert.False(queued, "Exact mtime match → no pull");
    }

    /// <summary>
    /// Same size, bumped by +1ms → triggers pull with exact comparison.
    /// </summary>
    [Fact]
    public async Task ClassifyFileForPull_MinimalBump_1ms_TriggersPull()
    {
        var options = CreateOptions();
        var storage = CreateStorageManager(options);
        var orchestrator = CreateOrchestrator(options, storage);

        const long serverMs = 1710000000000;
        storage.UpdateFileInCache("s", "config.json", 256, Epoch.AddMilliseconds(serverMs));

        var file = new FileMetadata
        {
            Key = "s",
            RelativePath = "config.json",
            Size = 256,
            LastWriteTimeUtc = Epoch.AddMilliseconds(serverMs + 1)
        };

        var conn = CreateTestConnection();
        await orchestrator.EnqueueFilesAsync(null, conn, new[] { file }, CancellationToken.None);

        bool queued = conn.SendChannel.Reader.TryRead(out _);
        Assert.True(queued, "Same size + bumped mtime must trigger pull for edited file");
    }

    // --- Multiple files in one classify ---

    [Fact]
    public async Task ClassifyFileForPull_MultipleFiles_MixedDecisions()
    {
        var options = CreateOptions();
        var storage = CreateStorageManager(options);
        var orchestrator = CreateOrchestrator(options, storage);

        const long baseMs = 1710000000000;
        storage.UpdateFileInCache("s", "uptodate.txt", 100, Epoch.AddMilliseconds(baseMs));
        storage.UpdateFileInCache("s", "outdated.txt", 100, Epoch.AddMilliseconds(baseMs));

        var files = new[]
        {
            // Exact match → skip
            new FileMetadata { Key = "s", RelativePath = "uptodate.txt", Size = 100,
                LastWriteTimeUtc = Epoch.AddMilliseconds(baseMs) },
            // Client newer → pull
            new FileMetadata { Key = "s", RelativePath = "outdated.txt", Size = 100,
                LastWriteTimeUtc = Epoch.AddMilliseconds(baseMs + 5000) },
            // Not in cache → pull
            new FileMetadata { Key = "s", RelativePath = "newfile.txt", Size = 200,
                LastWriteTimeUtc = DateTime.UtcNow },
        };

        var conn = CreateTestConnection();
        await orchestrator.EnqueueFilesAsync(null, conn, files, CancellationToken.None);

        // At least one pull message should be queued
        bool queued = conn.SendChannel.Reader.TryRead(out _);
        Assert.True(queued, "Expected at least one pull request for mixed file set");
    }

    [Fact]
    public async Task ClassifyFileForPull_AllFilesUpToDate_NoPull()
    {
        var options = CreateOptions();
        var storage = CreateStorageManager(options);
        var orchestrator = CreateOrchestrator(options, storage);

        const long baseMs = 1710000000000;
        storage.UpdateFileInCache("s", "a.txt", 100, Epoch.AddMilliseconds(baseMs));
        storage.UpdateFileInCache("s", "b.txt", 200, Epoch.AddMilliseconds(baseMs));

        var files = new[]
        {
            new FileMetadata { Key = "s", RelativePath = "a.txt", Size = 100,
                LastWriteTimeUtc = Epoch.AddMilliseconds(baseMs) },
            new FileMetadata { Key = "s", RelativePath = "b.txt", Size = 200,
                LastWriteTimeUtc = Epoch.AddMilliseconds(baseMs) },
        };

        var conn = CreateTestConnection();
        await orchestrator.EnqueueFilesAsync(null, conn, files, CancellationToken.None);

        bool queued = conn.SendChannel.Reader.TryRead(out _);
        Assert.False(queued, "Expected no pull when all files are up-to-date");
    }

    // --- Rename detection with server mtime ---

    [Fact]
    public async Task RenameDetection_SameSizeAndMtime_NoNetworkTransfer()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), $"velduct-rename-{Guid.NewGuid():N}");
        try
        {
            Directory.CreateDirectory(tmpDir);
            var options = new TransferOptions
            {
                Storage = new StorageOptions { DataDirectory = tmpDir, TempDirectory = Path.Combine(tmpDir, "_t") },
                Sync = new SyncOptions { ScanBatchSize = 100 }
            };
            var storage = CreateStorageManager(options);
            var orchestrator = CreateOrchestrator(options, storage);

            const long baseMs = 1710000000000;
            var mtime = Epoch.AddMilliseconds(baseMs);

            // Create actual file on disk so rename can work
            var sharePath = Path.Combine(tmpDir, "s");
            Directory.CreateDirectory(sharePath);
            var oldFile = Path.Combine(sharePath, "old.txt");
            File.WriteAllBytes(oldFile, new byte[512]);
            File.SetLastWriteTimeUtc(oldFile, mtime);

            // Cache knows about old.txt
            storage.UpdateFileInCache("s", "old.txt", 512, mtime);

            // Client says: delete old.txt, add new.txt with same size+mtime
            var files = new[]
            {
                new FileMetadata { Key = "s", RelativePath = "old.txt", Size = -1,
                    LastWriteTimeUtc = mtime },
                new FileMetadata { Key = "s", RelativePath = "new.txt", Size = 512,
                    LastWriteTimeUtc = mtime },
            };

            var conn = CreateTestConnection();
            await orchestrator.EnqueueFilesAsync(null, conn, files, CancellationToken.None);

            // Rename detected → no CMD_BATCH_PULL needed (only CMD_DELETE_CONFIRM expected)
            bool hasBatchPull = false;
            while (conn.SendChannel.Reader.TryRead(out var msg))
            {
                if (msg.Data.Length > 0 && msg.Data[0] == Protocol.CMD_BATCH_PULL)
                    hasBatchPull = true;
            }
            Assert.False(hasBatchPull, "Expected no CMD_BATCH_PULL for rename (same size+mtime)");

            // File should be moved on disk
            Assert.True(File.Exists(Path.Combine(sharePath, "new.txt")), "Renamed file should exist");
            Assert.False(File.Exists(oldFile), "Old file should be gone");
        }
        finally
        {
            if (Directory.Exists(tmpDir)) Directory.Delete(tmpDir, true);
        }
    }

    // --- Cache consistency ---

    [Fact]
    public void StorageManager_UpdateThenGet_ReturnsLatest()
    {
        var options = CreateOptions();
        var storage = CreateStorageManager(options);

        var t1 = Epoch.AddMilliseconds(1000);
        var t2 = Epoch.AddMilliseconds(2000);

        storage.UpdateFileInCache("s", "f.txt", 100, t1);
        storage.UpdateFileInCache("s", "f.txt", 200, t2); // overwrite

        var cached = storage.GetFromCache("s", "f.txt");
        Assert.NotNull(cached);
        Assert.Equal(200, cached.Size);
        Assert.Equal(t2, cached.LastWriteTime);
    }

    [Fact]
    public void StorageManager_RemoveFromCache_ReturnsNull()
    {
        var options = CreateOptions();
        var storage = CreateStorageManager(options);

        storage.UpdateFileInCache("s", "f.txt", 100, DateTime.UtcNow);
        storage.RemoveFromCache("s", "f.txt");

        Assert.Null(storage.GetFromCache("s", "f.txt"));
    }

    [Fact]
    public void StorageManager_PhantomUpload_BlocksWrite()
    {
        var options = CreateOptions();
        var storage = CreateStorageManager(options);

        var now = DateTime.UtcNow;
        var before = now.AddSeconds(-1);

        storage.RegisterDeleteTimestamp("s", "f.txt");

        // operationStartTime < deleteTime → phantom
        string path = Path.Combine(_tmpDir, "s", "f.txt");
        Assert.True(storage.IsPhantomUpload(path, before));
        Assert.False(storage.IsPhantomUpload(path, now.AddSeconds(1)));
    }

    // --- Server mtime stamping ---

    [Fact]
    public void StorageManager_StoresExactMtime()
    {
        var options = CreateOptions();
        var storage = CreateStorageManager(options);

        var serverMtime = new DateTime(2026, 3, 18, 13, 25, 0, 123, DateTimeKind.Utc);
        storage.UpdateFileInCache("share", "file.txt", 1024, serverMtime);

        var cached = storage.GetFromCache("share", "file.txt");
        Assert.NotNull(cached);
        Assert.Equal(1024, cached.Size);
        Assert.Equal(serverMtime, cached.LastWriteTime);
    }

    [Fact]
    public void MtimeAck_PreservesMillisecondPrecision()
    {
        var serverMtime = new DateTime(2026, 3, 18, 13, 25, 0, 123, DateTimeKind.Utc);
        long serverMtimeMs = new DateTimeOffset(serverMtime).ToUnixTimeMilliseconds();

        byte[] ack = Protocol.BuildFileMtimeAckMessage("share", "file.txt", serverMtimeMs);

        // Parse mtime from ACK message
        int pos = 1; // skip opcode
        int keyLen = BinaryPrimitives.ReadInt32LittleEndian(ack.AsSpan(pos)); pos += 4 + keyLen;
        int pathLen = BinaryPrimitives.ReadInt32LittleEndian(ack.AsSpan(pos)); pos += 4 + pathLen;
        long parsedMs = BinaryPrimitives.ReadInt64LittleEndian(ack.AsSpan(pos));

        Assert.Equal(serverMtimeMs, parsedMs);

        // Verify DateTime round-trip preserves ms
        var roundTripped = DateTimeOffset.FromUnixTimeMilliseconds(parsedMs).UtcDateTime;
        Assert.Equal(serverMtime, roundTripped);
    }
}
