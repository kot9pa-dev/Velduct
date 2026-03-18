using Velduct.Web.Configuration;
using Velduct.Web.Core;
using Velduct.Web.Domain;
using Velduct.Web.Infrastructure;
using Velduct.Web.Messaging;

namespace Velduct.Web.Services;

// Handles single-file receives; stores state across CMD_OFFER_FILE → CMD_FILE_DATA → CMD_FILE_DONE
public sealed class SingleFileReceiver : IAsyncDisposable
{
    private readonly ILogger<SingleFileReceiver> _logger;
    private readonly StorageManager _storage;
    private readonly TransferOptions _options;

    private FileStream _currentStream;
    private string _tempPath;
    private string _finalPath;
    private FileMetadata _currentFile;

    public SingleFileReceiver(
        ILogger<SingleFileReceiver> logger,
        StorageManager storage,
        Microsoft.Extensions.Options.IOptions<TransferOptions> options)
    {
        _logger = logger;
        _storage = storage;
        _options = options.Value;
    }

    public bool IsReceiving => _currentStream != null;

    public async Task HandleOfferAsync(WebSocketConnection conn, byte[] payload, CancellationToken ct)
    {
        await CloseCurrentFileAsync();

        int pos = 0;
        int keyLen = BitConverter.ToInt32(payload, pos); pos += 4;
        string key = System.Text.Encoding.UTF8.GetString(payload, pos, keyLen); pos += keyLen;
        int pathLen = BitConverter.ToInt32(payload, pos); pos += 4;
        string path = System.Text.Encoding.UTF8.GetString(payload, pos, pathLen); pos += pathLen;
        long size = BitConverter.ToInt64(payload, pos); pos += 8;
        long mtimeMs = BitConverter.ToInt64(payload, pos);

        _currentFile = new FileMetadata
        {
            Key = key,
            RelativePath = path.Replace("\\", "/"),
            Size = size,
            LastWriteTimeUtc = DateTimeOffset.FromUnixTimeMilliseconds(mtimeMs).UtcDateTime
        };

        _finalPath = Path.Combine(_options.Storage.DataDirectory, key, _currentFile.RelativePath);

        int lastSlash = _currentFile.RelativePath.LastIndexOf('/');
        string dirPath = lastSlash >= 0 ? _currentFile.RelativePath.Substring(0, lastSlash) : "";
        var dirNode = _storage.GetOrCreateDirectoryNode(key, dirPath);
        _storage.EnsureDirectoryCreated(dirNode);

        _tempPath = _finalPath + AppConstants.UploadExtension;

        try
        {
            // Буфер 4096 — данные пишутся чанками из WebSocket, не нужен большой внутренний буфер.
            var fs = new FileStream(_tempPath, FileMode.Create, FileAccess.Write,
                FileShare.None, 4096, true);
            if (_currentFile.Size > 0)
                fs.SetLength(_currentFile.Size);
            _currentStream = fs;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to open temp file for {RelPath}, aborting offer.", _currentFile.RelativePath);
            _currentFile = null;
            _tempPath = null;
            _finalPath = null;
            return;
        }

        for (int i = 0; i < _options.Network.MaxCredits; i++)
        {
            conn.EnqueueSend(
                new byte[] { Protocol.SRV_PULL_STREAM },
                System.Net.WebSockets.WebSocketMessageType.Binary,
                true);
        }

        _logger.LogDebug("Offer accepted for {Key}/{RelPath}, size={Size}.", key, _currentFile.RelativePath, size);
    }

    /// <summary>
    /// Обрабатывает входящий чанк CMD_FILE_DATA: пишет в открытый FileStream.
    /// </summary>
    public async Task HandleDataAsync(WebSocketConnection conn, byte[] headerBuffer, int offset, int count, CancellationToken ct)
    {
        if (_currentStream == null)
        {
            _logger.LogWarning("CMD_FILE_DATA received but no file is being received, ignoring chunk of {Count} bytes.", count);
            return;
        }

        await _currentStream.WriteAsync(headerBuffer.AsMemory(offset, count), ct);
        conn.EnqueueSend(
            new byte[] { Protocol.SRV_PULL_STREAM },
            System.Net.WebSockets.WebSocketMessageType.Binary,
            true);
    }

    /// <summary>
    /// Обрабатывает CMD_FILE_DONE: закрывает поток и ставит задачу на перемещение файла.
    /// </summary>
    public async Task HandleDoneAsync(TransferSession session)
    {
        if (_currentStream == null || _currentFile == null)
        {
            _logger.LogWarning("CMD_FILE_DONE received but no file transfer was in progress.");
            return;
        }

        await _currentStream.DisposeAsync();
        _currentStream = null;

        var task = new FileDiskTask(
            _currentFile.Key,
            _currentFile.RelativePath,
            _currentFile.Size,
            _currentFile.LastWriteTimeUtc,
            _finalPath,
            _tempPath);

        await session.CreateTasks.Writer.WriteAsync(task);

        _logger.LogDebug("File transfer complete for {Key}/{RelPath}, queued for disk write.",
            _currentFile.Key, _currentFile.RelativePath);

        _currentFile = null;
        _finalPath = null;
        _tempPath = null;
    }

    /// <summary>
    /// Закрывает текущий поток и удаляет незавершённый временный файл.
    /// </summary>
    public async Task CloseCurrentFileAsync()
    {
        if (_currentStream != null)
        {
            await _currentStream.DisposeAsync();
            _currentStream = null;
        }

        if (_tempPath != null && File.Exists(_tempPath))
        {
            try
            {
                File.Delete(_tempPath);
                _logger.LogDebug("Cleaned up incomplete temp file: {TempPath}", _tempPath);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Failed to delete incomplete temp file: {TempPath}", _tempPath);
            }
            _tempPath = null;
        }

        _currentFile = null;
        _finalPath = null;
    }

    public async ValueTask DisposeAsync() => await CloseCurrentFileAsync();
}