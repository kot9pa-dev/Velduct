using Microsoft.Extensions.Options;
using Velduct.Web.Configuration;
using Velduct.Web.Infrastructure;

namespace Velduct.Web.Services;

// Independent, one-way liveness. Every KeepAliveSeconds this sends a heartbeat to
// each live connection (so clients can detect a dead server), and aborts any
// connection whose peer heartbeat stopped arriving within ClientTimeoutSeconds.
// No request/response — each side asserts liveness on its own timer.
// Reaping is disabled when ClientTimeoutSeconds <= 0.
public sealed class ConnectionHealthMonitorService : BackgroundService
{
    private readonly ConnectionTracker _tracker;
    private readonly ILogger<ConnectionHealthMonitorService> _logger;
    private readonly TimeSpan _interval;
    private readonly long _clientTimeoutMs;
    private readonly bool _reap;

    public ConnectionHealthMonitorService(
        ConnectionTracker tracker,
        IOptions<TransferOptions> options,
        ILogger<ConnectionHealthMonitorService> logger)
    {
        _tracker = tracker;
        _logger = logger;
        var ws = options.Value.WebSocket;
        _interval = TimeSpan.FromSeconds(Math.Max(1, ws.KeepAliveSeconds));
        _clientTimeoutMs = ws.ClientTimeoutSeconds * 1000L;
        _reap = ws.ClientTimeoutSeconds > 0;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Connection health monitor started (heartbeat={Interval}s, clientTimeout={Timeout}s, reap={Reap}).",
            _interval.TotalSeconds, _clientTimeoutMs / 1000, _reap);

        using var timer = new PeriodicTimer(_interval);
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            var cutoff = Environment.TickCount64 - _clientTimeoutMs;
            foreach (var entry in _tracker.Snapshot())
            {
                if (_reap && entry.LastActivityMs < cutoff)
                {
                    _logger.LogWarning("Aborting dead client (no inbound activity > {Timeout}s).", _clientTimeoutMs / 1000);
                    try { entry.Abort(); }
                    catch (Exception ex) { _logger.LogDebug("Abort failed: {Message}", ex.Message); }
                }
                else
                {
                    try { entry.SendHeartbeat(); }
                    catch (Exception ex) { _logger.LogDebug("Heartbeat send failed: {Message}", ex.Message); }
                }
            }
        }
    }
}
