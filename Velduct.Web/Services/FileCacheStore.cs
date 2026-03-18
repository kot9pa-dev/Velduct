using System.Collections.Concurrent;
using Velduct.Web.Domain;

namespace Velduct.Web.Services;

public class FileCacheStore
{
    public ConcurrentDictionary<string, DirectoryNode> Shares { get; } = new(StringComparer.OrdinalIgnoreCase);
    public ConcurrentDictionary<string, DateTime> DeleteTimestamps { get; } = new(StringComparer.OrdinalIgnoreCase);
    public bool IsReady { get; set; } = false;
}
