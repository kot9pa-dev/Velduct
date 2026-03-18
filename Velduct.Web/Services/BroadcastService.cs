using System.Net.WebSockets;
using Velduct.Web.Core;
using Velduct.Web.Domain;
using Velduct.Web.Infrastructure;

namespace Velduct.Web.Services;

public sealed class BroadcastService
{
    private readonly ILogger<BroadcastService> _logger;
    private readonly ConnectionManager _connectionManager;

    public BroadcastService(ILogger<BroadcastService> logger, ConnectionManager connectionManager)
    {
        _logger = logger;
        _connectionManager = connectionManager;
    }

    public void NotifyFileWritten(Guid sourceConnectionId, string key, string relPath, long size, DateTime lastWriteTimeUtc)
    {
        var files = new List<FileMetadata>(1)
        {
            new()
            {
                Key = key,
                RelativePath = relPath,
                Size = size,
                LastWriteTimeUtc = lastWriteTimeUtc
            }
        };

        BroadcastCheckFiles(sourceConnectionId, key, files);
    }

    public void NotifyFilesWritten(Guid sourceConnectionId, string shareKey, IReadOnlyList<FileMetadata> files)
    {
        if (files.Count == 0) return;
        BroadcastCheckFiles(sourceConnectionId, shareKey, files);
    }

    public void NotifyFilesDeleted(Guid sourceConnectionId, string shareKey, IReadOnlyList<string> relativePaths)
    {
        if (relativePaths.Count == 0) return;

        var files = new List<FileMetadata>(relativePaths.Count);
        foreach (var relPath in relativePaths)
        {
            files.Add(new FileMetadata
            {
                Key = shareKey,
                RelativePath = relPath,
                Size = -1,
                LastWriteTimeUtc = DateTime.UtcNow
            });
        }

        BroadcastCheckFiles(sourceConnectionId, shareKey, files);
    }

    public void NotifyFileRenamed(Guid sourceConnectionId, string shareKey,
        string oldRelPath, string newRelPath, long size, DateTime lastWriteTimeUtc)
    {
        var files = new List<FileMetadata>(2)
        {
            new()
            {
                Key = shareKey,
                RelativePath = oldRelPath,
                Size = -1,
                LastWriteTimeUtc = DateTime.UtcNow
            },
            new()
            {
                Key = shareKey,
                RelativePath = newRelPath,
                Size = size,
                LastWriteTimeUtc = lastWriteTimeUtc
            }
        };

        BroadcastCheckFiles(sourceConnectionId, shareKey, files);
    }

    private void BroadcastCheckFiles(Guid sourceConnectionId, string shareKey, IReadOnlyList<FileMetadata> files)
    {
        byte[] message = Protocol.BuildCheckFilesMessage(files);
        int sent = 0;

        foreach (var client in _connectionManager.GetOtherClientsForShare(sourceConnectionId, shareKey))
        {
            if (client.Connection.Socket.State != WebSocketState.Open)
                continue;

            client.Connection.EnqueueSend(message, WebSocketMessageType.Binary, true);
            sent++;
        }

        if (sent > 0)
        {
            _logger.LogDebug("Broadcast {Count} file updates for share '{Share}' to {Clients} client(s).",
                files.Count, shareKey, sent);
        }
    }

}
