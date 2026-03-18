using System.Collections.Concurrent;

namespace Velduct.Web.Infrastructure;

// Tracks active client connections; broadcasts file updates to peers on the same share
public sealed class ConnectionManager
{
    private readonly ConcurrentDictionary<Guid, ClientInfo> _clients = new();

    public void RegisterClient(WebSocketConnection connection, TransferSession session)
    {
        _clients[connection.Id] = new ClientInfo
        {
            Connection = connection,
            Session = session
        };
    }

    public void RegisterShares(Guid connectionId, IEnumerable<string> shareKeys)
    {
        if (!_clients.TryGetValue(connectionId, out var info))
            return;

        foreach (var key in shareKeys)
            info.ShareKeys.TryAdd(key, 0);
    }

    public void UnregisterClient(Guid connectionId)
    {
        _clients.TryRemove(connectionId, out _);
    }

    // Returns clients subscribed to the share, excluding the source connection
    public IEnumerable<ClientInfo> GetOtherClientsForShare(Guid excludeConnectionId, string shareKey)
    {
        foreach (var kvp in _clients)
        {
            if (kvp.Key != excludeConnectionId && kvp.Value.ShareKeys.ContainsKey(shareKey))
                yield return kvp.Value;
        }
    }

    public int ClientCount => _clients.Count;
}
