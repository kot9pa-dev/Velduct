using System.Collections.Concurrent;

namespace Velduct.Web.Domain;

public class DirectoryNode
{
    public string FullPath { get; init; } = "";

    // Avoids repeated Directory.CreateDirectory calls
    public volatile bool IsCreatedOnDisk;

    // Subdirectories; concurrencyLevel=1, capacity=1 minimizes per-node memory since most are small
    public ConcurrentDictionary<string, DirectoryNode> Directories { get; } = new(1, 1, StringComparer.OrdinalIgnoreCase);

    public ConcurrentDictionary<string, CachedFileInfo> Files { get; } = new(1, 1, StringComparer.OrdinalIgnoreCase);
}
