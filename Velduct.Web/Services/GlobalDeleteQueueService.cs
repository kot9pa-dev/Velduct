using System.Threading.Channels;
using Velduct.Web.Configuration;
using Velduct.Web.Domain;
using Microsoft.Extensions.Options;

namespace Velduct.Web.Services;

// Global delete retry queue; persists across client disconnections and retries via background worker
public sealed class GlobalDeleteQueueService : IHostedService
{
    private readonly ILogger<GlobalDeleteQueueService> _logger;
    private readonly BroadcastService _broadcast;
    private readonly Channel<PendingDelete> _pendingDeletes;
    private readonly TransferOptions _options;
    private Task _workerTask;
    private CancellationTokenSource _cts;

    public GlobalDeleteQueueService(
        ILogger<GlobalDeleteQueueService> logger,
        BroadcastService broadcast,
        IOptions<TransferOptions> options)
    {
        _logger = logger;
        _broadcast = broadcast;
        _options = options.Value;
        _pendingDeletes = Channel.CreateUnbounded<PendingDelete>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });
    }

    public void Enqueue(PendingDelete item)
    {
        _pendingDeletes.Writer.TryWrite(item);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _workerTask = Task.Run(() => RunDeleteRetryWorkerAsync(_cts.Token));
        _logger.LogInformation("Global delete queue worker started.");
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _pendingDeletes.Writer.TryComplete();
        _cts?.Cancel();
        if (_workerTask != null)
            await _workerTask;
        _logger.LogInformation("Global delete queue worker stopped.");
    }

    private async Task RunDeleteRetryWorkerAsync(CancellationToken ct)
    {
        int totalRetried = 0;

        try
        {
            await foreach (var pending in _pendingDeletes.Reader.ReadAllAsync(ct))
            {
                int attempt = 0;

                while (!ct.IsCancellationRequested)
                {
                    if (!File.Exists(pending.FullPath) && !Directory.Exists(pending.FullPath))
                    {
                        _broadcast.NotifyFilesDeleted(
                            pending.SourceConnectionId, pending.Key,
                            new List<string> { pending.RelativePath });
                        _logger.LogDebug("Deferred delete resolved (path gone): {Key}/{RelPath}.",
                            pending.Key, pending.RelativePath);
                        break;
                    }

                    attempt++;

                    if (TryDeletePathFromDisk(pending.FullPath, pending.RelativePath))
                    {
                        _broadcast.NotifyFilesDeleted(
                            pending.SourceConnectionId, pending.Key,
                            new List<string> { pending.RelativePath });
                        totalRetried++;
                        _logger.LogInformation("Deferred delete succeeded after {Attempt} retries: {Key}/{RelPath}.",
                            attempt, pending.Key, pending.RelativePath);
                        break;
                    }

                    if (attempt == 1)
                    {
                        _logger.LogDebug("Retrying delete: {Key}/{RelPath} (file locked).",
                            pending.Key, pending.RelativePath);
                    }
                    else if (attempt % 120 == 0)
                    {
                        _logger.LogWarning("Still waiting to delete {Key}/{RelPath} — locked for {Seconds}s.",
                            pending.Key, pending.RelativePath, attempt * _options.Sync.DeleteRetryDelayMs / 1000);
                    }

                    await Task.Delay(_options.Sync.DeleteRetryDelayMs, ct);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Global delete retry worker crashed.");
        }

        _logger.LogDebug("Global delete retry worker stopped. Retried {Count} files.", totalRetried);
    }

    private bool TryDeletePathFromDisk(string path, string relPath)
    {
        if (Directory.Exists(path))
        {
            try { Directory.Delete(path, true); return true; }
            catch (IOException) { return false; }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to delete directory {Path}.", relPath);
                return false;
            }
        }

        if (File.Exists(path))
        {
            try { File.Delete(path); return true; }
            catch (IOException) { return false; }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to delete file {Path}.", relPath);
                return false;
            }
        }

        return true; // Already gone
    }
}

public readonly record struct PendingDelete(
    Guid SourceConnectionId,
    string Key,
    string RelativePath,
    string FullPath);
