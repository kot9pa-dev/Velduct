using System.Threading.Channels;
using Velduct.Web.Configuration;
using Velduct.Web.Domain;
using Velduct.Web.Messaging;
using Microsoft.Extensions.Options;

namespace Velduct.Web.Services;

// Global batch broadcaster; aggregates updates and flushes at configurable interval
public sealed class GlobalBroadcastFlusherService : IHostedService
{
    private readonly ILogger<GlobalBroadcastFlusherService> _logger;
    private readonly BroadcastService _broadcast;
    private readonly Channel<BroadcastEntry> _queue;
    private readonly TransferOptions _options;
    private Task _flusherTask;
    private CancellationTokenSource _cts;

    public GlobalBroadcastFlusherService(
        ILogger<GlobalBroadcastFlusherService> logger,
        BroadcastService broadcast,
        IOptions<TransferOptions> options)
    {
        _logger = logger;
        _broadcast = broadcast;
        _options = options.Value;
        _queue = Channel.CreateUnbounded<BroadcastEntry>(new UnboundedChannelOptions
        {
            SingleReader = true
        });
    }

    public void Enqueue(BroadcastEntry entry)
    {
        _queue.Writer.TryWrite(entry);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _flusherTask = Task.Run(() => RunFlusherAsync(_cts.Token));
        _logger.LogInformation("Global broadcast flusher started (interval={Interval}ms).", _options.Broadcast.FlushIntervalMs);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _queue.Writer.TryComplete();
        _cts?.Cancel();
        if (_flusherTask != null)
            await _flusherTask;
        _logger.LogInformation("Global broadcast flusher stopped.");
    }

    private async Task RunFlusherAsync(CancellationToken ct)
    {
        var accumulated = new List<BroadcastEntry>(64);
        var groups = new Dictionary<(Guid, string), List<FileMetadata>>();
        var listPool = new List<List<FileMetadata>>(8);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                BroadcastEntry first;
                try
                {
                    first = await _queue.Reader.ReadAsync(ct);
                }
                catch (ChannelClosedException)
                {
                    break;
                }

                accumulated.Add(first);
                await Task.Delay(_options.Broadcast.FlushIntervalMs, ct);

                while (_queue.Reader.TryRead(out var entry))
                {
                    accumulated.Add(entry);
                }

                FlushAccumulated(accumulated, groups, listPool);
                accumulated.Clear();
            }
        }
        catch (OperationCanceledException) { }

        while (_queue.Reader.TryRead(out var leftover))
        {
            accumulated.Add(leftover);
        }
        if (accumulated.Count > 0)
        {
            FlushAccumulated(accumulated, groups, listPool);
        }
    }

    private void FlushAccumulated(
        List<BroadcastEntry> entries,
        Dictionary<(Guid, string), List<FileMetadata>> groups,
        List<List<FileMetadata>> listPool)
    {
        if (entries.Count == 0) return;

        foreach (var e in entries)
        {
            var groupKey = (e.SourceConnectionId, e.Key);
            if (!groups.TryGetValue(groupKey, out var list))
            {
                if (listPool.Count > 0)
                {
                    list = listPool[listPool.Count - 1];
                    listPool.RemoveAt(listPool.Count - 1);
                }
                else
                {
                    list = new List<FileMetadata>();
                }
                groups[groupKey] = list;
            }
            list.Add(new FileMetadata
            {
                Key = e.Key,
                RelativePath = e.RelativePath,
                Size = e.Size,
                LastWriteTimeUtc = e.LastWriteTimeUtc
            });
        }

        foreach (var kvp in groups)
        {
            _broadcast.NotifyFilesWritten(kvp.Key.Item1, kvp.Key.Item2, kvp.Value);
        }

        _logger.LogDebug("Broadcast flusher: sent {Total} file notifications in {Groups} batch(es).",
            entries.Count, groups.Count);

        foreach (var kvp in groups)
        {
            kvp.Value.Clear();
            listPool.Add(kvp.Value);
        }
        groups.Clear();
    }
}
