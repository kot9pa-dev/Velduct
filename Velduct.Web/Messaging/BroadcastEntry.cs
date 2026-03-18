namespace Velduct.Web.Messaging;

// Deferred batch broadcast entry; struct avoids heap alloc on Channel enqueue
public readonly struct BroadcastEntry
{
    public readonly Guid SourceConnectionId;
    public readonly string Key;
    public readonly string RelativePath;
    public readonly long Size;
    public readonly DateTime LastWriteTimeUtc;

    public BroadcastEntry(Guid sourceConnectionId, string key, string relativePath, long size, DateTime lastWriteTimeUtc)
    {
        SourceConnectionId = sourceConnectionId;
        Key = key;
        RelativePath = relativePath;
        Size = size;
        LastWriteTimeUtc = lastWriteTimeUtc;
    }
}
