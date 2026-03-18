using System.Net.WebSockets;
using Microsoft.Extensions.Options;
using Velduct.Web.Configuration;
using Velduct.Web.Core;
using Velduct.Web.Infrastructure;

namespace Velduct.Web.Services;
public class FileTransferService
{
    private readonly ILogger<FileTransferService> _logger;
    private readonly StorageManager _storage;
    private readonly TransferOptions _options;

    public FileTransferService(ILogger<FileTransferService> logger, StorageManager storage, IOptions<TransferOptions> options)
    {
        _logger = logger;
        _storage = storage;
        _options = options.Value;
    }

    public async Task ReceiveFileAsync(WebSocket ws, TransferSession session, string localPath, string shareName, string relPath, long expectedSize, DateTime mtime, CancellationToken ct)
    {
        string tempFilePath = Path.Combine(_options.Storage.TempDirectory, Guid.NewGuid().ToString() + AppConstants.UploadExtension);

        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(localPath)!);
            using (var fs = new FileStream(tempFilePath, FileMode.Create, FileAccess.Write, FileShare.None, 4096, true))
            {
                for (int i = 0; i < _options.Network.MaxCredits; i++)
                {
                    await ws.SendAsync(new byte[] { Protocol.SRV_PULL_STREAM }, WebSocketMessageType.Binary, true, ct);
                }

                bool isDone = false;

                while (!isDone && ws.State == WebSocketState.Open)
                {
                    var payload = await session.DataChannel.Reader.ReadAsync(ct);
                    if (payload == null || payload.Length == 0) break;

                    byte opCode = payload[0];

                    if (opCode == Protocol.CMD_FILE_DATA)
                    {
                        await fs.WriteAsync(payload.AsMemory(1, payload.Length - 1), ct);
                        await ws.SendAsync(new byte[] { Protocol.SRV_PULL_STREAM }, WebSocketMessageType.Binary, true, ct);
                    }
                    else if (opCode == Protocol.CMD_FILE_DONE)
                    {
                        isDone = true;
                    }
                }
                await fs.FlushAsync(ct);
            }

            if (new FileInfo(tempFilePath).Length == expectedSize)
            {
                if (File.Exists(localPath)) File.Delete(localPath);
                File.Move(tempFilePath, localPath);
                File.SetLastWriteTimeUtc(localPath, mtime);

                _storage.UpdateFileInCache(shareName, relPath, expectedSize, mtime);
            }
        }
        finally
        {
            if (File.Exists(tempFilePath)) File.Delete(tempFilePath);
        }
    }
}