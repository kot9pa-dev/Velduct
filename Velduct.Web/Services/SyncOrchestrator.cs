using System.Net.WebSockets;
using System.Threading.Channels;
using Velduct.Web.Core;
using Velduct.Web.Domain;
using Velduct.Web.Infrastructure;

namespace Velduct.Web.Services;

// Sync orchestrator; classifies files, detects renames, queues deletes, broadcasts updates
public class SyncOrchestrator
{
    private readonly ILogger<SyncOrchestrator> _logger;
    private readonly StorageManager _storage;
    private readonly Velduct.Web.Configuration.TransferOptions _options;
    private readonly BroadcastService _broadcast;
    private readonly GlobalDeleteQueueService _deleteQueue;

    public SyncOrchestrator(
        ILogger<SyncOrchestrator> logger,
        StorageManager storage,
        Microsoft.Extensions.Options.IOptions<Velduct.Web.Configuration.TransferOptions> options,
        BroadcastService broadcast,
        GlobalDeleteQueueService deleteQueue)
    {
        _logger = logger;
        _storage = storage;
        _options = options.Value;
        _broadcast = broadcast;
        _deleteQueue = deleteQueue;
    }

    public Task EnqueueFilesAsync(WebSocket ws, WebSocketConnection conn, IEnumerable<FileMetadata> files, CancellationToken ct)
    {
        var (filesToPull, deletedFiles, directDeletes) = ClassifyFiles(files);

        ProcessRenames(conn.Id, filesToPull, deletedFiles);

        var confirmedDeletes = ExecuteDeletes(conn.Id, deletedFiles, directDeletes);

        if (confirmedDeletes.Count > 0)
            SendDeleteConfirm(conn, confirmedDeletes);

        if (filesToPull.Count > 0)
        {
            _logger.LogDebug("Requesting {Count} files from client in batches of {BatchSize}.",
                filesToPull.Count, _options.Sync.ScanBatchSize);

            foreach (var chunk in filesToPull.Chunk(_options.Sync.ScanBatchSize))
                SendBatchPullRequest(conn, chunk.ToList());
        }

        return Task.CompletedTask;
    }

    // --- Classify ---

    private (List<FileMetadata> filesToPull, List<FileMetadata> deletedFiles, List<FileMetadata> directDeletes)
        ClassifyFiles(IEnumerable<FileMetadata> files)
    {
        var filesToPullMap = new Dictionary<(string, string), FileMetadata>();
        var deletedFiles = new List<FileMetadata>();
        var directDeletes = new List<FileMetadata>();

        foreach (var file in files)
        {
            if (file.Size == -1)
            {
                HandleDeleteSignal(file, deletedFiles, directDeletes);
                continue;
            }

            if (file.Size == -2)
            {
                HandleDirectoryCreate(file);
                continue;
            }

            ClassifyFileForPull(file, filesToPullMap);
        }

        return (filesToPullMap.Values.ToList(), deletedFiles, directDeletes);
    }

    private void HandleDeleteSignal(FileMetadata file, List<FileMetadata> deletedFiles, List<FileMetadata> directDeletes)
    {
        var subs = _storage.GetAllFilesUnderPath(file.Key, file.RelativePath);
        _logger.LogDebug("Delete signal for {Key}/{Path}: resolved to {Count} cached files.",
            file.Key, file.RelativePath, subs.Count);

        deletedFiles.AddRange(subs);
        directDeletes.Add(file);
    }

    private void HandleDirectoryCreate(FileMetadata file)
    {
        var dirPath = Path.Combine(_options.Storage.DataDirectory, file.Key, file.RelativePath);
        try
        {
            Directory.CreateDirectory(dirPath);
            var dirNode = _storage.GetOrCreateDirectoryNode(file.Key, file.RelativePath);
            _storage.EnsureDirectoryCreated(dirNode);
            _logger.LogDebug("Created directory {Key}/{Path}.", file.Key, file.RelativePath);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to create directory {Key}/{Path}.", file.Key, file.RelativePath);
        }
    }

    private void ClassifyFileForPull(FileMetadata file, Dictionary<(string, string), FileMetadata> pullMap)
    {
        var cached = _storage.GetFromCache(file.Key, file.RelativePath);

        if (cached == null)
        {
            pullMap[(file.Key, file.RelativePath)] = file;
            return;
        }

        long serverMtimeMs = new DateTimeOffset(cached.LastWriteTime).ToUnixTimeMilliseconds();
        long clientMtimeMs = new DateTimeOffset(file.LastWriteTimeUtc).ToUnixTimeMilliseconds();

        // Epsilon 2s covers FS mtime rounding (FAT32 → 2s, HFS+ → 1s)
        if (cached.Size == file.Size && Math.Abs(serverMtimeMs - clientMtimeMs) <= 2000)
            return;

        if (clientMtimeMs > serverMtimeMs)
        {
            pullMap[(file.Key, file.RelativePath)] = file;
        }
        else
        {
            _logger.LogDebug("Server has newer version of {Key}/{Path} (serverMs={Server} > clientMs={Client}), skipping pull.",
                file.Key, file.RelativePath, serverMtimeMs, clientMtimeMs);
        }
    }


    private void ProcessRenames(Guid sourceConnectionId, List<FileMetadata> filesToPull, List<FileMetadata> deletedFiles)
    {
        if (deletedFiles.Count == 0 || filesToPull.Count == 0)
            return;

        int renamesFound = 0;

        for (int i = filesToPull.Count - 1; i >= 0; i--)
        {
            var pull = filesToPull[i];
            long pullMs = new DateTimeOffset(pull.LastWriteTimeUtc, TimeSpan.Zero).ToUnixTimeMilliseconds();

            var match = deletedFiles.FirstOrDefault(d =>
                d.Size == pull.Size &&
                new DateTimeOffset(d.LastWriteTimeUtc, TimeSpan.Zero).ToUnixTimeMilliseconds() == pullMs);

            if (match != null && TryApplyRename(sourceConnectionId, match, pull))
            {
                filesToPull.RemoveAt(i);
                deletedFiles.Remove(match);
                renamesFound++;
            }
        }

        if (renamesFound > 0)
            _logger.LogDebug("Atomic rename detection: resolved {Count} rename(s) without network transfer.", renamesFound);
    }

    private bool TryApplyRename(Guid sourceConnectionId, FileMetadata oldFile, FileMetadata newFile)
    {
        var oldPath = Path.Combine(_options.Storage.DataDirectory, oldFile.Key, oldFile.RelativePath);
        var newPath = Path.Combine(_options.Storage.DataDirectory, newFile.Key, newFile.RelativePath);

        try
        {
            var newDir = Path.GetDirectoryName(newPath);
            if (!string.IsNullOrEmpty(newDir))
                Directory.CreateDirectory(newDir);

            File.Move(oldPath, newPath, overwrite: true);

            _storage.RemoveFromCache(oldFile.Key, oldFile.RelativePath);
            _storage.UpdateFileInCache(newFile.Key, newFile.RelativePath, newFile.Size, newFile.LastWriteTimeUtc);

            _logger.LogDebug("Renamed: {OldKey}/{OldPath} => {NewKey}/{NewPath}",
                oldFile.Key, oldFile.RelativePath, newFile.Key, newFile.RelativePath);

            // Notify other clients of rename
            _broadcast.NotifyFileRenamed(
                sourceConnectionId,
                newFile.Key,
                oldFile.RelativePath,
                newFile.RelativePath,
                newFile.Size,
                newFile.LastWriteTimeUtc);

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Rename from {OldPath} to {NewPath} failed, falling back to network pull.",
                oldFile.RelativePath, newFile.RelativePath);
            return false;
        }
    }

    // --- Deletes ---

    private List<string> ExecuteDeletes(Guid sourceConnectionId, List<FileMetadata> deletedFiles, List<FileMetadata> directDeletes)
    {
        var confirmed = new List<string>();
        var deletedByShare = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

        foreach (var f in deletedFiles.Concat(directDeletes))
        {
            _storage.RegisterDeleteTimestamp(f.Key, f.RelativePath);

            var path = Path.Combine(_options.Storage.DataDirectory, f.Key, f.RelativePath);
            bool deleted = TryDeletePathFromDisk(path, f.RelativePath);

            if (deleted)
            {
                // Deleted immediately; remove from cache and confirm
                _storage.RemoveFromCache(f.Key, f.RelativePath);
                confirmed.Add($"{f.Key}/{f.RelativePath}");

                if (!deletedByShare.TryGetValue(f.Key, out var list))
                {
                    list = new List<string>();
                    deletedByShare[f.Key] = list;
                }
                list.Add(f.RelativePath);
            }
            else
            {
                // File locked; enqueue for retry. Don't remove from cache or confirm to client.
                _deleteQueue.Enqueue(new PendingDelete(
                    sourceConnectionId, f.Key, f.RelativePath, path));

                _logger.LogDebug("Delete of {Key}/{RelPath} deferred to retry worker (file locked).",
                    f.Key, f.RelativePath);
            }
        }

        if (confirmed.Count > 0)
            _logger.LogDebug("Deleted {Count} paths from disk and cache.", confirmed.Count);

        // Notify other clients of deletions
        foreach (var (shareKey, paths) in deletedByShare)
        {
            _broadcast.NotifyFilesDeleted(sourceConnectionId, shareKey, paths);
        }

        return confirmed;
    }

    /// <summary>
    // Single delete attempt; returns true if deleted or not found
    /// </summary>
    private bool TryDeletePathFromDisk(string path, string relPath)
    {
        if (Directory.Exists(path))
        {
            try
            {
                Directory.Delete(path, true);
                _logger.LogDebug("Deleted directory: {Path}", relPath);
                return true;
            }
            catch (IOException ex)
            {
                _logger.LogDebug("Directory locked, deferring: {Path}. Reason: {Message}", relPath, ex.Message);
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to delete directory {Path}.", relPath);
                return false;
            }
        }

        if (File.Exists(path))
        {
            try
            {
                File.Delete(path);
                _logger.LogDebug("Deleted file: {Path}", relPath);
                return true;
            }
            catch (IOException ex)
            {
                _logger.LogDebug("File locked, deferring: {Path}. Reason: {Message}", relPath, ex.Message);
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to delete file {Path}.", relPath);
                return false;
            }
        }

        _logger.LogDebug("Path not found on disk (already deleted?): {Path}", relPath);
        return true; // Нет на диске — считаем удалённым
    }

    // --- Network messages ---

    private void SendBatchPullRequest(WebSocketConnection conn, List<FileMetadata> files)
    {
        byte[] message = Protocol.BuildBatchPullMessage(files);
        conn.EnqueueSend(message, WebSocketMessageType.Binary, true);
    }

    private void SendDeleteConfirm(WebSocketConnection conn, List<string> paths)
    {
        _logger.LogDebug("Sending CMD_DELETE_CONFIRM for {Count} paths.", paths.Count);
        byte[] message = Protocol.BuildDeleteConfirmMessage(paths);
        conn.EnqueueSend(message, WebSocketMessageType.Binary, true);
    }
}
