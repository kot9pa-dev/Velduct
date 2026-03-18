using Microsoft.Extensions.Logging;
using System;
using System.Formats.Tar;
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Velduct.Web.Configuration;
using Velduct.Web.Extensions;
using Velduct.Web.Infrastructure;
using Velduct.Web.Messaging;

namespace Velduct.Web.Services;

public sealed class ArchiveProcessor
{
    private readonly ILogger<ArchiveProcessor> _logger;
    private readonly StorageManager _storage;
    private readonly MemoryManagerService _memory;
    private readonly TransferOptions _options;

    public ArchiveProcessor(
        ILogger<ArchiveProcessor> logger,
        StorageManager storage,
        MemoryManagerService memory,
        Microsoft.Extensions.Options.IOptions<TransferOptions> options)
    {
        _logger = logger;
        _storage = storage;
        _memory = memory;
        _options = options.Value;
    }

    public bool HasActiveArchive(TransferSession session) => session.ArchiveChannel != null;

    // Awaits previous unpack task before starting new one to recover buffers to pool; prevents deadlock
    public async Task StartArchiveAsync(TransferSession session, CancellationToken ct)
    {
        await CancelAndAwaitPreviousArchiveAsync(session);

        var unpackCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        session.ArchiveChannel = Channel.CreateBounded<ArchiveChunk>(new BoundedChannelOptions(_options.Network.MaxCredits)
        {
            FullMode = BoundedChannelFullMode.Wait
        });

        session.UnpackCts = unpackCts;

        var stream = session.ArchiveChannel.Reader.AsObservableStream(_memory.NetworkBufferPool.Writer);

        session.UnpackTask = Task.Run(async () =>
        {
            try
            {
                await UnpackTarAsync(session, stream, unpackCts.Token);
            }
            catch (OperationCanceledException)
            {
                _logger.LogDebug("Archive unpack cancelled (new archive or session close).");
            }
            catch (Exception ex)
            {
                _logger.LogDebug("Archive unpack task finished: {Message}", ex.Message);
            }
            finally
            {
                stream.Dispose();

                // Return remaining buffered chunks to pool
                if (session.ArchiveChannel != null)
                {
                    session.ArchiveChannel.Writer.TryComplete();
                    while (session.ArchiveChannel.Reader.TryRead(out var leftover))
                    {
                        _memory.NetworkBufferPool.Writer.TryWrite(leftover.Data);
                    }
                }
            }
        }, CancellationToken.None);

        session.Connection.EnqueueSend(
            Core.Protocol.BuildCreditMessage(_options.Network.MaxCredits),
            System.Net.WebSockets.WebSocketMessageType.Binary,
            true);

        _logger.LogDebug("Archive channel opened, issued {Credits} initial credits.", _options.Network.MaxCredits);
    }

    // Graceful shutdown: complete channel to trigger EOF, then await; use CTS.Cancel as timeout fallback
    private async Task CancelAndAwaitPreviousArchiveAsync(TransferSession session)
    {
        if (session.ArchiveChannel != null)
        {
            session.ArchiveChannel.Writer.TryComplete();

            // Drain chunks to return buffers before awaiting (ChannelStream may block on read)
            while (session.ArchiveChannel.Reader.TryRead(out var stuck))
            {
                _memory.NetworkBufferPool.Writer.TryWrite(stuck.Data);
            }
        }

        if (session.UnpackTask != null)
        {
            _logger.LogDebug("Awaiting previous archive unpack task before starting new one...");

            try
            {
                await session.UnpackTask.WaitAsync(
                    TimeSpan.FromMilliseconds(_options.Archive.UnpackTaskTimeoutMs));
                _logger.LogDebug("Previous archive unpack task completed, buffers returned to pool.");
            }
            catch (TimeoutException)
            {
                _logger.LogWarning("Previous archive unpack task did not complete gracefully within {Timeout}ms, forcing cancellation.", _options.Archive.UnpackTaskTimeoutMs);

                if (session.UnpackCts != null)
                {
                    try { session.UnpackCts.Cancel(); }
                    catch (ObjectDisposedException) { }
                }

                // Ждём ещё немного после Cancel
                try
                {
                    await session.UnpackTask.WaitAsync(TimeSpan.FromMilliseconds(2000));
                }
                catch (TimeoutException)
                {
                    _logger.LogError("Previous archive unpack task did NOT complete even after cancellation! Buffer leak possible.");
                    _ = session.UnpackTask.ContinueWith(_ =>
                        _logger.LogWarning("Late archive unpack task finally completed (after forced cancel)."),
                        TaskContinuationOptions.ExecuteSynchronously);
                }
            }
        }

        // Очищаем состояние
        if (session.UnpackCts != null)
        {
            try { session.UnpackCts.Dispose(); }
            catch (ObjectDisposedException) { }
            session.UnpackCts = null;
        }

        session.UnpackTask = null;
        session.ArchiveChannel = null;
    }

    public void CompleteArchive(TransferSession session)
    {
        if (session.ArchiveChannel == null)
        {
            _logger.LogInformation("CMD_ARCHIVE_DONE received but no active archive channel.");
            return;
        }

        session.ArchiveChannel.Writer.TryComplete();
        session.ArchiveChannel = null;
        _logger.LogDebug("Archive channel closed by CMD_ARCHIVE_DONE.");
    }

    public async Task FlushChunkAsync(
        TransferSession session,
        byte[] buffer,
        int written,
        CancellationToken ct)
    {
        if (session.ArchiveChannel == null)
        {
            _logger.LogDebug("Archive chunk received but no active archive channel, returning buffer to pool.");
            _memory.NetworkBufferPool.Writer.TryWrite(buffer);
            return;
        }

        try
        {
            await session.ArchiveChannel.Writer.WriteAsync(new ArchiveChunk(buffer, written), ct);
            session.Connection.EnqueueSend(
                Core.Protocol.CreditMessage,
                System.Net.WebSockets.WebSocketMessageType.Binary,
                true);
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("Archive channel write cancelled (session closing), returning buffer to pool.");
            _memory.NetworkBufferPool.Writer.TryWrite(buffer);
        }
        catch (ChannelClosedException)
        {
            _logger.LogDebug("Archive channel was closed before chunk could be written, returning buffer to pool.");
            _memory.NetworkBufferPool.Writer.TryWrite(buffer);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error writing archive chunk, returning buffer to pool.");
            _memory.NetworkBufferPool.Writer.TryWrite(buffer);
        }
    }

    public async Task DrainAndReleaseBuffersAsync(TransferSession session)
    {
        await CancelAndAwaitPreviousArchiveAsync(session);
    }

    private async Task UnpackTarAsync(TransferSession session, Stream tarStream, CancellationToken ct)
    {
        try
        {
            Directory.CreateDirectory(_options.Storage.TempDirectory);
            using var bufferedStream = new BufferedStream(tarStream, _options.Disk.TarReadBufferSize);
            using var reader = new TarReader(bufferedStream);

            int processed = 0;
            int skipped = 0;
            long totalBytes = 0;

            while (await reader.GetNextEntryAsync(false, ct) is TarEntry entry)
            {
                if (entry.EntryType != TarEntryType.RegularFile && entry.EntryType != TarEntryType.V7RegularFile)
                    continue;

                var name = entry.Name;
                int slashIdx = name.IndexOf('/');
                if (slashIdx <= 0 || slashIdx >= name.Length - 1)
                {
                    _logger.LogDebug("Skipping TAR entry with unexpected name format: {Name}", name);
                    continue;
                }

                string key = name.Substring(0, slashIdx);
                string relPath = name.Substring(slashIdx + 1);
                string finalPath = System.IO.Path.Combine(_options.Storage.DataDirectory, key, relPath);
                DateTime processingStartTime = DateTime.UtcNow;

                if (_storage.IsPhantomUpload(finalPath, processingStartTime))
                {
                    _logger.LogDebug("Skipping phantom TAR entry {RelPath} (deleted mid-flight).", relPath);
                    skipped++;
                    continue;
                }

                totalBytes += entry.Length;
                await ExtractEntryAsync(session, entry, key, relPath, finalPath, processingStartTime, ct);
                processed++;
            }

            _logger.LogInformation("TAR unpack complete: {Processed} files, {Skipped} skipped, {SizeMB:F2} MB.",
                processed, skipped, totalBytes / (1024.0 * 1024.0));
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("TAR unpack cancelled (session closed).");
        }
        catch (EndOfStreamException)
        {
            _logger.LogWarning("TAR stream ended unexpectedly — possible connection drop during archive transfer.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Fatal error unpacking TAR stream.");
        }
    }

    /// <summary>
    /// Извлекает один файл из TAR: всегда пишет во временный файл в TempDirectory,
    /// затем DiskWorkerService атомарно перенесёт его в финальный путь через File.Move.
    /// </summary>
    private async Task ExtractEntryAsync(
        TransferSession session,
        TarEntry entry,
        string key,
        string relPath,
        string finalPath,
        DateTime processingStartTime,
        CancellationToken ct)
    {
        int lastSlash = relPath.LastIndexOf('/');
        string dirPath = lastSlash >= 0 ? relPath.Substring(0, lastSlash) : "";
        var dirNode = _storage.GetOrCreateDirectoryNode(key, dirPath);
        _storage.EnsureDirectoryCreated(dirNode);

        string tempPath = System.IO.Path.Combine(
            _options.Storage.TempDirectory,
            System.IO.Path.GetRandomFileName() + Core.AppConstants.UploadExtension);

        try
        {
            // Буфер 4096 — минимальный, т.к. CopyToAsync использует свой буфер для throughput.
            // IoBufferSize (256KB+) на LOH → накапливаются до GC2.
            await using (var fs = new FileStream(tempPath, FileMode.Create, FileAccess.Write,
                FileShare.None, 4096,
                FileOptions.Asynchronous | FileOptions.SequentialScan))
            {
                if (entry.Length > 0)
                    fs.SetLength(entry.Length);

                if (entry.DataStream != null)
                    await entry.DataStream.CopyToAsync(fs, _options.Disk.IoBufferSize, ct);
            }

            // Server-authoritative mtime: stamp server time as the canonical version timestamp.
            var serverMtime = DateTime.UtcNow;

            var task = new FileDiskTask(key, relPath, entry.Length,
                serverMtime, finalPath, tempPath, processingStartTime);

            await session.CreateTasks.Writer.WriteAsync(task, ct);
        }
        catch (OperationCanceledException)
        {
            CleanupTempFile(tempPath, relPath);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to write TAR entry {RelPath} to temp file.", relPath);
            CleanupTempFile(tempPath, relPath);
        }
    }

    private void CleanupTempFile(string tempPath, string relPath)
    {
        if (!File.Exists(tempPath))
            return;

        try
        {
            File.Delete(tempPath);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to clean up temp file for {RelPath}.", relPath);
        }
    }
}