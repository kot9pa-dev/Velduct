using System.Net.WebSockets;
using System.Text;
using System.Threading.Channels;
using Velduct.Web.Configuration;
using Velduct.Web.Core;
using Velduct.Web.Domain;
using Velduct.Web.Infrastructure;
using Velduct.Web.Messaging;
using Microsoft.Extensions.Options;

namespace Velduct.Web.Services;

// WebSocket session orchestrator; routes opcodes to specialized handlers
public class WebSocketHandlerService : IFrameHandler
{
    private readonly ILogger<WebSocketHandlerService> _logger;
    private readonly SyncOrchestrator _sync;
    private readonly StorageManager _storage;
    private readonly TransferOptions _options;
    private readonly MemoryManagerService _memory;
    private readonly WebSocketFrameReader _frameReader;
    private readonly ArchiveProcessor _archiveProcessor;
    private readonly DiskWorkerService _diskWorker;
    private readonly ConnectionManager _connectionManager;
    private readonly ServerArchiveSender _archiveSender;
    private readonly ConnectionTracker _connectionTracker;

    private TransferSession _session;
    private SingleFileReceiver _fileReceiver;
    private ConnectionTracker.Entry _connEntry;

    // Queue for reverse sync (file pull requests); ensures one TAR at a time without blocking ReadLoop
    private readonly Channel<List<FileMetadata>> _pullRequestChannel =
        Channel.CreateUnbounded<List<FileMetadata>>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true
        });

    public WebSocketHandlerService(
        ILogger<WebSocketHandlerService> logger,
        SyncOrchestrator sync,
        StorageManager storage,
        IOptions<TransferOptions> options,
        MemoryManagerService memory,
        WebSocketFrameReader frameReader,
        ArchiveProcessor archiveProcessor,
        DiskWorkerService diskWorker,
        ConnectionManager connectionManager,
        ServerArchiveSender archiveSender,
        ConnectionTracker connectionTracker)
    {
        _logger = logger;
        _sync = sync;
        _storage = storage;
        _options = options.Value;
        _memory = memory;
        _frameReader = frameReader;
        _archiveProcessor = archiveProcessor;
        _diskWorker = diskWorker;
        _connectionManager = connectionManager;
        _archiveSender = archiveSender;
        _connectionTracker = connectionTracker;
    }

    public async Task HandleAsync(WebSocket webSocket, CancellationToken ct)
    {
        var connection = new WebSocketConnection(webSocket, _options.Network.MaxCredits, _options.Network.SendChannelCapacity);
        _session = new TransferSession(connection, _options.Network.MaxCredits, _options.Disk.CreateTasksQueueCapacity, _options.Disk.FinalizeTasksQueueCapacity);
        _fileReceiver = new SingleFileReceiver(
            _logger.IsEnabled(LogLevel.Debug)
                ? _logger as ILogger<SingleFileReceiver> ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<SingleFileReceiver>.Instance
                : Microsoft.Extensions.Logging.Abstractions.NullLogger<SingleFileReceiver>.Instance,
            _storage,
            Microsoft.Extensions.Options.Options.Create(_options));

        _connectionManager.RegisterClient(connection, _session);
        _connEntry = _connectionTracker.Add(connection.Id,
            abort: () => { try { connection.Socket.Abort(); } catch { /* already gone */ } },
            sendHeartbeat: () => connection.EnqueueSend(Protocol.HeartbeatMessage, WebSocketMessageType.Binary, true));

        _logger.LogInformation("Client connected (id={Id}). Starting session (MaxCredits={Credits}, ChunkSize={Chunk}KB). Active clients: {Total}.",
            connection.Id, _options.Network.MaxCredits, _options.Network.ChunkSizeBytes / 1024, _connectionManager.ClientCount);

        var sendLoop = RunSendLoopAsync(connection, ct);
        var finalizeWorker = _diskWorker.RunFinalizeWorkerAsync(_session, ct);
        var createWorkers = _diskWorker.RunAllCreateWorkersAsync(_session, ct);
        var pullWorker = RunPullWorkerAsync(connection, ct);

        try
        {
            await _frameReader.ReadLoopAsync(webSocket, this, ct);
        }
        catch (WebSocketException ex)
        {
            _logger.LogInformation("Client connection dropped (WebSocket error): {Message}", ex.Message);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Connection closed via server cancellation.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected session error.");
        }
        finally
        {
            await CleanupSessionAsync();
        }
    }

    // --- IFrameHandler implementation ---

    public Task OnCreditsAsync(int count, CancellationToken ct)
    {
        _connEntry?.Touch();
        for (int i = 0; i < count; i++)
        {
            _session!.Connection.OutboundCredits.Writer.TryWrite(1);
        }
        return Task.CompletedTask;
    }

    public Task<bool> HasActiveArchiveAsync()
        => Task.FromResult(_archiveProcessor.HasActiveArchive(_session!));

    public Task OnArchiveChunkAsync(byte[] buffer, int written, CancellationToken ct)
    {
        _connEntry?.Touch(); // hot path: one atomic write, no lookup
        return _archiveProcessor.FlushChunkAsync(_session!, buffer, written, ct);
    }

    public async Task OnFullMessageAsync(byte opCode, byte[] payload, CancellationToken ct)
    {
        _connEntry?.Touch();
        switch (opCode)
        {
            case Protocol.CMD_HEARTBEAT:
                break; // client liveness heartbeat; activity already recorded above

            case Protocol.CMD_CHECK_FILES:
                await HandleCheckFilesAsync(_session!.Connection, payload, ct);
                break;

            case Protocol.CMD_REGISTER_SHARES:
                HandleRegisterShares(payload);
                break;

            case Protocol.CMD_BATCH_PULL:
                // Enqueue pull request; PullWorker sends TAR streams sequentially without blocking ReadLoop
                EnqueuePullRequest(payload);
                break;

            case Protocol.CMD_OFFER_FILE:
                await _fileReceiver!.HandleOfferAsync(_session!.Connection, payload, ct);
                break;

            case Protocol.CMD_FILE_DATA:
                await _fileReceiver!.HandleDataAsync(_session!.Connection, payload, 0, payload.Length, ct);
                break;

            case Protocol.CMD_FILE_DONE:
                await _fileReceiver!.HandleDoneAsync(_session!);
                break;

            case Protocol.CMD_ARCHIVE_RAW_START:
                await _archiveProcessor.StartArchiveAsync(_session!, ct);
                break;

            case Protocol.CMD_ARCHIVE_DONE:
                _archiveProcessor.CompleteArchive(_session!);
                break;

            default:
                _logger.LogDebug("Unknown opcode 0x{OpCode:X2} received, ignoring.", opCode);
                break;
        }
    }

    // --- Handlers ---

    private async Task HandleCheckFilesAsync(WebSocketConnection conn, byte[] payload, CancellationToken ct)
    {
        int pos = 0;
        if (payload.Length < 4)
        {
            _logger.LogWarning("CMD_CHECK_FILES payload too short ({Length} bytes), ignoring.", payload.Length);
            return;
        }

        int count = BitConverter.ToInt32(payload, pos); pos += 4;

        // Bounds validation: reject absurdly large counts to prevent OOM/DoS.
        // Each file entry is at least 24 bytes (4+1 + 4+1 + 8 + 8).
        const int MinEntrySize = 24;
        int maxPossible = (payload.Length - pos) / MinEntrySize;
        if (count < 0 || count > maxPossible)
        {
            _logger.LogWarning("CMD_CHECK_FILES: invalid count {Count} (max possible {Max} for payload length {Len}), ignoring.",
                count, maxPossible, payload.Length);
            return;
        }

        var files = new List<FileMetadata>(Math.Min(count, 10000));

        for (int i = 0; i < count; i++)
        {
            if (pos + 4 > payload.Length) break;
            int keyLen = BitConverter.ToInt32(payload, pos); pos += 4;
            if (keyLen < 0 || pos + keyLen > payload.Length) break;
            string key = Encoding.UTF8.GetString(payload, pos, keyLen); pos += keyLen;

            if (pos + 4 > payload.Length) break;
            int pathLen = BitConverter.ToInt32(payload, pos); pos += 4;
            if (pathLen < 0 || pos + pathLen > payload.Length) break;
            string path = Encoding.UTF8.GetString(payload, pos, pathLen); pos += pathLen;

            if (pos + 16 > payload.Length) break;
            long size = BitConverter.ToInt64(payload, pos); pos += 8;
            long mtimeMs = BitConverter.ToInt64(payload, pos); pos += 8;

            files.Add(new FileMetadata
            {
                Key = key,
                RelativePath = path.Replace("\\", "/"),
                Size = size,
                LastWriteTimeUtc = DateTimeOffset.FromUnixTimeMilliseconds(mtimeMs).UtcDateTime
            });
        }

        _logger.LogDebug("CMD_CHECK_FILES: received metadata for {Count} files.", count);
        await _sync.EnqueueFilesAsync(_session!.Connection.Socket, conn, files, ct);
    }

    private void HandleRegisterShares(byte[] payload)
    {
        if (payload.Length < 4)
        {
            _logger.LogWarning("CMD_REGISTER_SHARES payload too short.");
            return;
        }

        int pos = 0;
        int count = BitConverter.ToInt32(payload, pos); pos += 4;
        if (count < 0 || count > 1000)
        {
            _logger.LogWarning("CMD_REGISTER_SHARES: invalid count {Count}, ignoring.", count);
            return;
        }
        var keys = new List<string>(count);

        for (int i = 0; i < count; i++)
        {
            if (pos + 4 > payload.Length) break;
            int keyLen = BitConverter.ToInt32(payload, pos); pos += 4;
            if (keyLen < 0 || keyLen > 1024 || pos + keyLen > payload.Length) break;
            keys.Add(Encoding.UTF8.GetString(payload, pos, keyLen));
            pos += keyLen;
        }

        _connectionManager.RegisterShares(_session!.Connection.Id, keys);
        _logger.LogInformation("Client {Id} registered {Count} shares: [{Keys}].",
            _session!.Connection.Id, keys.Count, string.Join(", ", keys));

        SendCachedFilesToClient(keys);
    }

    private void SendCachedFilesToClient(List<string> shareKeys)
    {
        int totalSent = 0;

        foreach (var key in shareKeys)
        {
            var allFiles = _storage.GetAllFilesUnderPath(key, "");
            if (allFiles.Count == 0)
                continue;

            // Отправляем батчами чтобы не создавать огромные сообщения
            foreach (var chunk in allFiles.Chunk(_options.Sync.ScanBatchSize))
            {
                var batch = chunk.ToList();
                byte[] message = Protocol.BuildCheckFilesMessage(batch);
                _session!.Connection.EnqueueSend(
                    message,
                    System.Net.WebSockets.WebSocketMessageType.Binary,
                    true);
                totalSent += batch.Count;
            }
        }

        if (totalSent > 0)
        {
            _logger.LogDebug("Initial sync: sent {Count} cached file entries to client {Id}.",
                totalSent, _session!.Connection.Id);
        }
    }

    private void EnqueuePullRequest(byte[] payload)
    {
        if (payload.Length < 4)
        {
            _logger.LogWarning("CMD_BATCH_PULL from client: payload too short.");
            return;
        }

        int pos = 0;
        int count = BitConverter.ToInt32(payload, pos); pos += 4;
        const int MinEntrySize = 24;
        int maxPossible = (payload.Length - pos) / MinEntrySize;
        if (count < 0 || count > maxPossible)
        {
            _logger.LogWarning("CMD_BATCH_PULL: invalid count {Count}, ignoring.", count);
            return;
        }
        var files = new List<FileMetadata>(Math.Min(count, 10000));

        for (int i = 0; i < count; i++)
        {
            if (pos + 4 > payload.Length) break;
            int keyLen = BitConverter.ToInt32(payload, pos); pos += 4;
            if (keyLen < 0 || pos + keyLen > payload.Length) break;
            string key = Encoding.UTF8.GetString(payload, pos, keyLen); pos += keyLen;

            if (pos + 4 > payload.Length) break;
            int pathLen = BitConverter.ToInt32(payload, pos); pos += 4;
            if (pathLen < 0 || pos + pathLen > payload.Length) break;
            string path = Encoding.UTF8.GetString(payload, pos, pathLen); pos += pathLen;

            if (pos + 16 > payload.Length) break;
            long size = BitConverter.ToInt64(payload, pos); pos += 8;
            long mtimeMs = BitConverter.ToInt64(payload, pos); pos += 8;

            files.Add(new FileMetadata
            {
                Key = key,
                RelativePath = path.Replace("\\", "/"),
                Size = size,
                LastWriteTimeUtc = DateTimeOffset.FromUnixTimeMilliseconds(mtimeMs).UtcDateTime
            });
        }

        if (files.Count > 0)
        {
            _pullRequestChannel.Writer.TryWrite(files);
            _logger.LogDebug("CMD_BATCH_PULL enqueued: {Count} files.", files.Count);
        }
    }

    private async Task RunPullWorkerAsync(WebSocketConnection conn, CancellationToken ct)
    {
        try
        {
            await foreach (var firstBatch in _pullRequestChannel.Reader.ReadAllAsync(ct))
            {
                var combined = new List<FileMetadata>(firstBatch);

                // Coalescing: accumulate pull requests before creating a single TAR.
                // Mirrors client-side batch worker policy (sender.go drainLoop):
                //   - maxFiles  = ScanBatchSize * MaxCredits  (derived, same as client)
                //   - maxBytes  = MaxCredits * ChunkSizeBytes (derived, same as client)
                //   - idleGap   = PullCoalescingIdleMs        (config, default 250ms)
                //   - totalCap  = PullCoalescingMaxMs         (config, default 2500ms)
                int maxFiles = _options.Sync.ScanBatchSize * _options.Network.MaxCredits;
                long maxBytes = (long)_options.Network.MaxCredits * _options.Network.ChunkSizeBytes;
                long currentBytes = combined.Sum(f => f.Size);

                var totalDeadline = DateTime.UtcNow.AddMilliseconds(_options.Sync.PullCoalescingMaxMs);
                while (DateTime.UtcNow < totalDeadline
                       && combined.Count < maxFiles
                       && currentBytes < maxBytes)
                {
                    while (_pullRequestChannel.Reader.TryRead(out var moreBatch))
                    {
                        combined.AddRange(moreBatch);
                        currentBytes += moreBatch.Sum(f => f.Size);
                        if (combined.Count >= maxFiles || currentBytes >= maxBytes)
                            break;
                    }

                    // Trim excess: TryRead adds whole batch, may overshoot maxFiles.
                    // Return leftover to channel for next TAR.
                    if (combined.Count > maxFiles)
                    {
                        var excess = combined.GetRange(maxFiles, combined.Count - maxFiles);
                        combined.RemoveRange(maxFiles, combined.Count - maxFiles);
                        currentBytes = combined.Sum(f => f.Size);
                        _pullRequestChannel.Writer.TryWrite(excess);
                    }

                    if (combined.Count >= maxFiles || currentBytes >= maxBytes)
                        break;

                    var remaining = totalDeadline - DateTime.UtcNow;
                    if (remaining <= TimeSpan.Zero)
                        break;

                    // Wait up to IdleMs for next item — if nothing arrives, we're done coalescing
                    var idleTimeout = TimeSpan.FromMilliseconds(
                        Math.Min(_options.Sync.PullCoalescingIdleMs, remaining.TotalMilliseconds));
                    using var delayCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    delayCts.CancelAfter(idleTimeout);
                    try
                    {
                        if (await _pullRequestChannel.Reader.WaitToReadAsync(delayCts.Token))
                            continue;
                        break;
                    }
                    catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                    {
                        break; // Idle timeout — send what we have
                    }
                }

                var deduped = combined
                    .GroupBy(f => (f.Key, f.RelativePath))
                    .Select(g => g.Last())
                    .ToList();

                if (deduped.Count == 0)
                    continue;

                _logger.LogDebug("PullWorker: sending {Unique} files to client (merged from {Total} entries).",
                    deduped.Count, combined.Count);

                try
                {
                    await _archiveSender.SendFilesToClientAsync(conn, deduped, ct);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "PullWorker: TAR send failed for batch of {Count} files.", deduped.Count);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("PullWorker stopped (cancelled).");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "PullWorker: fatal error.");
        }
    }

    private async Task RunSendLoopAsync(WebSocketConnection conn, CancellationToken ct)
    {
        int messagesSent = 0;
        var reader = conn.SendChannel.Reader;

        try
        {
            await foreach (var msg in reader.ReadAllAsync(ct))
            {
                if (conn.Socket.State != WebSocketState.Open)
                {
                    _logger.LogInformation("Send loop: socket no longer open, stopping.");
                    ReturnToPool(msg);
                    return;
                }

                await conn.SendPermit.Reader.ReadAsync(ct);
                try
                {
                    await SendWithRetryAsync(conn, msg, ct);
                    messagesSent++;

                    while (reader.TryRead(out var nextMsg))
                    {
                        if (conn.Socket.State != WebSocketState.Open)
                        {
                            ReturnToPool(nextMsg);
                            return;
                        }
                        await SendWithRetryAsync(conn, nextMsg, ct);
                        messagesSent++;
                    }
                }
                finally
                {
                    conn.SendPermit.Writer.TryWrite(true);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("Send loop cancelled after {Count} messages.", messagesSent);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Fatal error in send loop after {Count} messages.", messagesSent);
        }
    }

    private async Task SendWithRetryAsync(WebSocketConnection conn, OutboundMessage msg, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            if (conn.Socket.State != WebSocketState.Open)
            {
                ReturnToPool(msg);
                return;
            }

            try
            {
                await conn.Socket.SendAsync(msg.AsMemory(), msg.Type, msg.EndOfMessage, ct);
                ReturnToPool(msg);
                return;
            }
            catch (WebSocketException ex)
            {
                _logger.LogDebug("WebSocket SendAsync failed, retrying in {Delay}ms: {Message}",
                    _options.WebSocket.SendRetryDelayMs, ex.Message);
                await Task.Delay(_options.WebSocket.SendRetryDelayMs, ct);
            }
        }

        ReturnToPool(msg);
    }

    private static void ReturnToPool(OutboundMessage msg)
    {
        msg.ReturnPool?.Writer.TryWrite(msg.Data);
    }

    private async Task CleanupSessionAsync()
    {
        if (_session != null)
        {
            _connectionManager.UnregisterClient(_session.Connection.Id);
            _connectionTracker.Remove(_session.Connection.Id);
        }

        if (_fileReceiver != null)
            await _fileReceiver.CloseCurrentFileAsync();

        if (_session != null)
            await _archiveProcessor.DrainAndReleaseBuffersAsync(_session);

        _pullRequestChannel.Writer.TryComplete();

        _session?.CreateTasks.Writer.TryComplete();
        _session?.FinalizeTasks.Writer.TryComplete();
        _session?.DataChannel.Writer.TryComplete();

        if (_session != null)
        {
            while (_session.Connection.SendChannel.Reader.TryRead(out var stuckMsg))
                ReturnToPool(stuckMsg);
        }
        _session?.Connection.SendChannel.Writer.TryComplete();
        _session?.Connection.OutboundCredits.Writer.TryComplete();
        _session?.Connection.SendPermit.Writer.TryComplete();

        var ws = _session?.Connection.Socket;
        if (ws != null && (ws.State == WebSocketState.Open || ws.State == WebSocketState.CloseReceived))
        {
            try
            {
                await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "Session ended", CancellationToken.None);
            }
            catch (Exception ex)
            {
                _logger.LogDebug("WebSocket close handshake failed (client may have disconnected): {Message}", ex.Message);
            }
        }

        _logger.LogDebug("Session cleaned up. Active clients: {Count}.", _connectionManager.ClientCount);
    }
}
