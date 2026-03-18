using System.Collections.Concurrent;

namespace Velduct.Web.Infrastructure;

public sealed class ClientInfo
{
    public required WebSocketConnection Connection { get; init; }
    public required TransferSession Session { get; init; }

    // Share keys subscribed to; populated once at CMD_REGISTER_SHARES, read lock-free thereafter
    public ConcurrentDictionary<string, byte> ShareKeys { get; } = new(StringComparer.OrdinalIgnoreCase);
}
