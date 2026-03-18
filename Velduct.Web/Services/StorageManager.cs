using Velduct.Web.Configuration;
using Velduct.Web.Domain;
using Velduct.Web.Core;

namespace Velduct.Web.Services;

public class StorageManager
{
    private readonly ILogger<StorageManager> _logger;
    private readonly FileCacheStore _store;
    private readonly TransferOptions _options;

    public StorageManager(ILogger<StorageManager> logger, FileCacheStore store,
        Microsoft.Extensions.Options.IOptions<TransferOptions> options)
    {
        _logger = logger;
        _store = store;
        _options = options.Value;
    }

    public bool IsReady => _store.IsReady;

    public async Task InitializeFullScanAsync()
    {
        _logger.LogInformation("Starting VFS storage scan of {DataDirectory}...", _options.Storage.DataDirectory);

        if (!Directory.Exists(_options.Storage.DataDirectory))
            Directory.CreateDirectory(_options.Storage.DataDirectory);

        CleanupTempDirectory();

        var shareDirs = Directory.GetDirectories(_options.Storage.DataDirectory);
        _logger.LogInformation("Found {Count} share directories.", shareDirs.Length);

        await Task.WhenAll(shareDirs.Select(shareDir => Task.Run(() => IndexShareDirectory(shareDir))));

        _store.IsReady = true;
        _logger.LogInformation("VFS indexing complete. Cache ready.");
    }

    private void CleanupTempDirectory()
    {
        var tempDir = _options.Storage.TempDirectory;
        if (!Directory.Exists(tempDir))
            return;

        int cleaned = 0;
        try
        {
            foreach (var file in Directory.GetFiles(tempDir, "*", SearchOption.TopDirectoryOnly))
            {
                try
                {
                    File.Delete(file);
                    cleaned++;
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Could not delete orphan temp file: {File}", file);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to scan TempDirectory for cleanup: {Dir}", tempDir);
        }

        if (cleaned > 0)
            _logger.LogInformation("Cleaned {Count} orphan temp files from {Dir}.", cleaned, tempDir);
    }

    private void IndexShareDirectory(string shareDir)
    {
        var shareName = Path.GetFileName(shareDir);

        _store.Shares.GetOrAdd(shareName, _ => new DirectoryNode
        {
            FullPath = shareDir,
            IsCreatedOnDisk = true
        });

        var files = Directory.GetFiles(shareDir, "*", SearchOption.AllDirectories);
        int cleanedUp = 0;
        int indexed = 0;

        foreach (var file in files)
        {
            if (file.EndsWith(AppConstants.UploadExtension) || file.EndsWith(AppConstants.TempExtension))
            {
                try
                {
                    File.Delete(file);
                    cleanedUp++;
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Could not delete orphan temp file: {File}", file);
                }
                continue;
            }

            int prefixLen = shareDir.Length;
            var relPath = file.Substring(prefixLen).TrimStart(Path.DirectorySeparatorChar).Replace("\\", "/");
            var info = new FileInfo(file);
            UpdateFileInCache(shareName, relPath, info.Length, info.LastWriteTimeUtc, isScannedFromDisk: true);
            indexed++;
        }

        _logger.LogInformation("Share '{ShareName}' indexed: {Indexed} files, {Cleaned} orphans cleaned.",
            shareName, indexed, cleanedUp);
    }

    public DirectoryNode GetOrCreateDirectoryNode(string shareName, string relDirPath)
    {
        // TryGetValue first for 0-alloc cache hit; GetOrAdd allocates Func + closure every time
        if (!_store.Shares.TryGetValue(shareName, out var node))
        {
            // static lambda + arg — нет closure, нет аллокации делегата при повторном вызове
            node = _store.Shares.GetOrAdd(shareName, static (k, dataDir) => new DirectoryNode
            {
                FullPath = Path.Combine(dataDir, k),
                IsCreatedOnDisk = false
            }, _options.Storage.DataDirectory);
        }

        if (string.IsNullOrEmpty(relDirPath) || relDirPath == "." || relDirPath == "/")
            return node;

        // Manual walk avoids string[] allocation from Split
        int start = 0;
        while (start < relDirPath.Length)
        {
            int slash = relDirPath.IndexOf('/', start);
            int end = slash >= 0 ? slash : relDirPath.Length;

            if (end > start)
            {
                var part = relDirPath.Substring(start, end - start);

                // TryGetValue first for 0-alloc cache hit
                if (!node.Directories.TryGetValue(part, out var child))
                {
                    child = node.Directories.GetOrAdd(part, static (k, parentPath) => new DirectoryNode
                    {
                        FullPath = Path.Combine(parentPath, k),
                        IsCreatedOnDisk = false
                    }, node.FullPath);
                }

                node = child;
            }

            start = end + 1;
        }

        return node;
    }

    private DirectoryNode TryGetDirectoryNode(string shareName, string relDirPath)
    {
        if (!_store.Shares.TryGetValue(shareName, out var node))
            return null;

        if (string.IsNullOrEmpty(relDirPath) || relDirPath == "." || relDirPath == "/")
            return node;

        // Manual walk avoids string[] allocation from Split
        int start = 0;
        while (start < relDirPath.Length)
        {
            int slash = relDirPath.IndexOf('/', start);
            int end = slash >= 0 ? slash : relDirPath.Length;

            if (end > start)
            {
                var part = relDirPath.Substring(start, end - start);
                if (!node.Directories.TryGetValue(part, out node!))
                    return null;
            }

            start = end + 1;
        }

        return node;
    }

    public void EnsureDirectoryCreated(DirectoryNode dirNode)
    {
        if (dirNode.IsCreatedOnDisk) return;
        Directory.CreateDirectory(dirNode.FullPath);
        dirNode.IsCreatedOnDisk = true;
    }

    // --- Cache CRUD ---

    public void UpdateFileInCache(string shareName, string relPath, long size, DateTime mtime, bool isScannedFromDisk = false)
    {
        int lastSlash = relPath.LastIndexOf('/');
        string dirPath = lastSlash >= 0 ? relPath.Substring(0, lastSlash) : "";
        string fileName = lastSlash >= 0 ? relPath.Substring(lastSlash + 1) : relPath;

        var dirNode = GetOrCreateDirectoryNode(shareName, dirPath);
        dirNode.IsCreatedOnDisk = true;
        dirNode.Files[fileName] = new CachedFileInfo(size, mtime);

        // Clear phantom protection on successful write; otherwise retry after delete is rejected
        ClearDeleteTimestamps(shareName, relPath);
    }

    public CachedFileInfo GetFromCache(string shareName, string relPath)
    {
        int lastSlash = relPath.LastIndexOf('/');
        string dirPath = lastSlash >= 0 ? relPath.Substring(0, lastSlash) : "";
        string fileName = lastSlash >= 0 ? relPath.Substring(lastSlash + 1) : relPath;

        // Reads must not create directories; use TryGet to avoid Func+closure alloc per call
        var dirNode = TryGetDirectoryNode(shareName, dirPath);
        if (dirNode == null) return null;
        return dirNode.Files.TryGetValue(fileName, out var info) ? info : null;
    }

    public void RemoveFromCache(string shareName, string relPath)
    {
        int lastSlash = relPath.LastIndexOf('/');
        string dirPath = lastSlash >= 0 ? relPath.Substring(0, lastSlash) : "";
        string fileName = lastSlash >= 0 ? relPath.Substring(lastSlash + 1) : relPath;

        var dirNode = GetOrCreateDirectoryNode(shareName, dirPath);
        dirNode.Files.TryRemove(fileName, out _);
        dirNode.Directories.TryRemove(fileName, out _);
    }

    public List<FileMetadata> GetAllFilesUnderPath(string shareName, string relPath)
    {
        var result = new List<FileMetadata>();

        var directFile = GetFromCache(shareName, relPath);
        if (directFile != null)
        {
            result.Add(new FileMetadata
            {
                Key = shareName,
                RelativePath = relPath,
                Size = directFile.Size,
                LastWriteTimeUtc = directFile.LastWriteTime
            });
            return result;
        }

        var node = TryGetDirectoryNode(shareName, relPath);
        if (node != null)
            CollectFilesRecursive(result, shareName, relPath, node);

        return result;
    }

    private void CollectFilesRecursive(List<FileMetadata> result, string shareName, string relParentPath, DirectoryNode node)
    {
        foreach (var file in node.Files)
        {
            string fullRel = string.IsNullOrEmpty(relParentPath) ? file.Key : relParentPath + "/" + file.Key;
            result.Add(new FileMetadata
            {
                Key = shareName,
                RelativePath = fullRel,
                Size = file.Value.Size,
                LastWriteTimeUtc = file.Value.LastWriteTime
            });
        }

        foreach (var dir in node.Directories)
        {
            string fullRel = string.IsNullOrEmpty(relParentPath) ? dir.Key : relParentPath + "/" + dir.Key;
            CollectFilesRecursive(result, shareName, fullRel, dir.Value);
        }
    }

    // --- Delete timestamps (phantom upload protection) ---

    public void RegisterDeleteTimestamp(string shareName, string relPath)
    {
        string fullPath = Path.Combine(_options.Storage.DataDirectory, shareName, relPath);
        _store.DeleteTimestamps[fullPath] = DateTime.UtcNow;
        _logger.LogDebug("Registered delete timestamp for {Key}/{Path}.", shareName, relPath);

        if (_store.DeleteTimestamps.Count > _options.Sync.DeleteTimestampCleanupThreshold)
            PurgeExpiredDeleteTimestamps();
    }

    public bool IsPhantomUpload(string finalPath, DateTime operationStartTime)
    {
        // Fast path: skip entire walk if no deleted paths; avoids 3-4 levels of Path.GetDirectoryName per file
        if (_store.DeleteTimestamps.IsEmpty) return false;

        var current = finalPath;
        while (!string.IsNullOrEmpty(current))
        {
            if (_store.DeleteTimestamps.TryGetValue(current, out var deleteTime) && deleteTime > operationStartTime)
            {
                _logger.LogDebug("Phantom upload detected: {Path} was deleted at {DeleteTime} after operation started at {StartTime}.",
                    finalPath, deleteTime, operationStartTime);
                return true;
            }
            current = Path.GetDirectoryName(current)!;
        }
        return false;
    }

    private void ClearDeleteTimestamps(string shareName, string relPath)
    {
        if (_store.DeleteTimestamps.IsEmpty) return;

        string fullPath = Path.Combine(_options.Storage.DataDirectory, shareName, relPath);
        var current = fullPath;

        while (!string.IsNullOrEmpty(current) && current.StartsWith(_options.Storage.DataDirectory))
        {
            _store.DeleteTimestamps.TryRemove(current, out _);
            current = Path.GetDirectoryName(current)!;
        }
    }

    private void PurgeExpiredDeleteTimestamps()
    {
        var threshold = DateTime.UtcNow - TimeSpan.FromMinutes(_options.Sync.DeleteTimestampTtlMinutes);
        int removed = 0;

        foreach (var kvp in _store.DeleteTimestamps)
        {
            if (kvp.Value < threshold && _store.DeleteTimestamps.TryRemove(kvp.Key, out _))
                removed++;
        }

        if (removed > 0)
            _logger.LogDebug("Purged {Count} expired delete timestamps.", removed);
    }
}