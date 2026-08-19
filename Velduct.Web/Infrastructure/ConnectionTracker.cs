using System.Collections.Concurrent;

namespace Velduct.Web.Infrastructure;

// Tracks last-activity per connection so a background sweeper can abort dead ones.
// Hot-path design: Add() returns a direct Entry handle; the per-connection handler
// keeps it and calls Touch() on each inbound frame — no dictionary lookup, no
// DateTime, just one atomic write. The dictionary is only touched on
// connect/disconnect and by the sweeper.
//
// NOTE: on raw WebSocket the framework hides ping/pong from the app, so activity
// is only observable from DATA frames. Idle-but-alive clients therefore send a
// periodic app-level heartbeat (CMD_HEARTBEAT) to keep their activity fresh.
public sealed class ConnectionTracker
{
    private readonly ConcurrentDictionary<Guid, Entry> _connections = new();

    public sealed class Entry
    {
        private long _lastActivityMs;

        public Action Abort { get; }
        public Action SendHeartbeat { get; }

        public Entry(Action abort, Action sendHeartbeat)
        {
            Abort = abort;
            SendHeartbeat = sendHeartbeat;
            _lastActivityMs = Environment.TickCount64;
        }

        // Hot path: single lock-free write, monotonic clock, no allocation.
        public void Touch() => Interlocked.Exchange(ref _lastActivityMs, Environment.TickCount64);

        public long LastActivityMs => Interlocked.Read(ref _lastActivityMs);
    }

    public Entry Add(Guid id, Action abort, Action sendHeartbeat)
    {
        var entry = new Entry(abort, sendHeartbeat);
        _connections[id] = entry;
        return entry; // caller keeps this and Touch()es it directly (no lookup)
    }

    public void Remove(Guid id) => _connections.TryRemove(id, out _);

    public IReadOnlyList<Entry> Snapshot() => new List<Entry>(_connections.Values);

    public int Count => _connections.Count;
}
