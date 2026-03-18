using System.Threading.Channels;
using Velduct.Web.Configuration;
using Velduct.Web.Core;
using Velduct.Web.Domain;
using Velduct.Web.Infrastructure;
using Velduct.Web.Messaging;

namespace Velduct.Web.Services;

// Background disk workers; batches notifications to prevent cascading small CmdCheckFiles/CmdBatchPull
public sealed class DiskWorkerService
{
    private readonly ILogger<DiskWorkerService> _logger;
    private readonly StorageManager _storage;
    private readonly TransferOptions _options;
    private readonly BroadcastService _broadcast;
    private readonly GlobalBroadcastFlusherService _globalFlusher;

    public DiskWorkerService(
        ILogger<DiskWorkerService> logger,
        StorageManager storage,
        Microsoft.Extensions.Options.IOptions<TransferOptions> options,
        BroadcastService broadcast,
        GlobalBroadcastFlusherService globalFlusher)
    {
        _logger = logger;
        _storage = storage;
        _options = options.Value;
        _broadcast = broadcast;
        _globalFlusher = globalFlusher;
    }

    public Task RunAllCreateWorkersAsync(TransferSession session, CancellationToken ct)
    {
        var workers = Enumerable
            .Range(0, _options.Disk.MaxConcurrentDiskIoTasks)
            .Select(i => RunCreateWorkerAsync(session, i, ct))
            .ToArray();

        return Task.WhenAll(workers);
    }

    public Task RunFinalizeWorkerAsync(TransferSession session, CancellationToken ct)
        => RunFinalizeWorkerCoreAsync(session, ct);

    // --- Create Worker ---

    private async Task RunCreateWorkerAsync(TransferSession session, int workerId, CancellationToken ct)
    {
        _logger.LogDebug("Create worker #{WorkerId} started.", workerId);
        int filesProcessed = 0;

        try
        {
            await foreach (var task in session.CreateTasks.Reader.ReadAllAsync(ct))
            {
                await ProcessCreateTaskAsync(session, task, ct);
                filesProcessed++;
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("Create worker #{WorkerId} cancelled after {Count} files.", workerId, filesProcessed);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Create worker #{WorkerId} crashed after {Count} files.", workerId, filesProcessed);
        }

        _logger.LogDebug("Create worker #{WorkerId} stopped. Processed {Count} files.", workerId, filesProcessed);
    }

    // Atomic move: TempPath → FinalPath
    private async Task ProcessCreateTaskAsync(TransferSession session, FileDiskTask task, CancellationToken ct)
    {
        try
        {
            if (_storage.IsPhantomUpload(task.FinalPath, task.OperationStartTime))
            {
                _logger.LogDebug("Move aborted for {RelPath} — path was deleted mid-flight.", task.RelativePath);
                TryDeleteFile(task.TempPath, task.RelativePath);
                return;
            }

            bool moveSuccess = await TryMoveWithRetryAsync(task, task.TempPath, ct);

            if (moveSuccess)
            {
                _storage.UpdateFileInCache(task.Key, task.RelativePath, task.Size, task.LastWriteTimeUtc);
                _logger.LogDebug("File written: {Key}/{RelPath} ({Size} bytes).",
                    task.Key, task.RelativePath, task.Size);

                // Enqueue for batched broadcast (with FlushIntervalMs delay)
                _globalFlusher.Enqueue(new BroadcastEntry(
                    session.Connection.Id,
                    task.Key,
                    task.RelativePath,
                    task.Size,
                    task.LastWriteTimeUtc));
            }
            else
            {
                TryDeleteFile(task.TempPath, task.RelativePath);
            }
        }
        catch (OperationCanceledException)
        {
            TryDeleteFile(task.TempPath, task.RelativePath);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Fatal CREATE error for {Key}/{RelPath}.", task.Key, task.RelativePath);
            TryDeleteFile(task.TempPath, task.RelativePath);
        }
    }

    private async Task<bool> TryMoveWithRetryAsync(FileDiskTask task, string tmpPath, CancellationToken ct)
    {
        int retries = 0;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var destDir = Path.GetDirectoryName(task.FinalPath);
                if (!string.IsNullOrEmpty(destDir) && !Directory.Exists(destDir))
                    Directory.CreateDirectory(destDir);

                File.Move(tmpPath, task.FinalPath, overwrite: true);

                try
                {
                    File.SetLastWriteTimeUtc(task.FinalPath, task.LastWriteTimeUtc);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Could not set mtime on {FinalPath}.", task.FinalPath);
                }

                return true;
            }
            catch (IOException ex)
            {
                retries++;
                if (retries == 1)
                    _logger.LogDebug("File locked for {RelPath} (attempt {Retry}), retrying every {Delay}ms. Reason: {Message}",
                        task.RelativePath, retries, _options.Disk.MoveRetryDelayMs, ex.Message);
                else if (retries % 10 == 0)
                    _logger.LogDebug("Still waiting to move {RelPath} after {Retry} attempts.", task.RelativePath, retries);

                await Task.Delay(_options.Disk.MoveRetryDelayMs, ct);
            }
            catch (Exception ex)
            {
                _logger.LogDebug("Move aborted for {RelPath} due to environmental change: {Message}",
                    task.RelativePath, ex.Message);
                return false;
            }
        }

        return false;
    }

    // --- Finalize Worker ---

    private async Task RunFinalizeWorkerCoreAsync(TransferSession session, CancellationToken ct)
    {
        _logger.LogDebug("Finalize worker started.");
        int filesProcessed = 0;

        try
        {
            await foreach (var task in session.FinalizeTasks.Reader.ReadAllAsync(ct))
            {
                try
                {
                    File.SetLastWriteTimeUtc(task.FinalPath, task.LastWriteTimeUtc);
                    _storage.UpdateFileInCache(task.Key, task.RelativePath, task.Size, task.LastWriteTimeUtc);

                    // Enqueue for batched broadcast
                    _globalFlusher.Enqueue(new BroadcastEntry(
                        session.Connection.Id,
                        task.Key,
                        task.RelativePath,
                        task.Size,
                        task.LastWriteTimeUtc));

                    filesProcessed++;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Finalize failed for {Key}/{RelPath}.", task.Key, task.RelativePath);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("Finalize worker cancelled after {Count} files.", filesProcessed);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Finalize worker crashed.");
        }

        _logger.LogDebug("Finalize worker stopped. Processed {Count} files.", filesProcessed);
    }

    private void TryDeleteFile(string path, string relPath)
    {
        if (!File.Exists(path))
            return;

        try
        {
            File.Delete(path);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to clean up temp file for {RelPath}.", relPath);
        }
    }
}
