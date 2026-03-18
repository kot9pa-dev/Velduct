namespace Velduct.Web.Domain;

public class FileMetadata
{
    public string Key { get; set; } = string.Empty;
    public string RelativePath { get; set; } = string.Empty;
    public long Size { get; set; }
    public DateTime LastWriteTimeUtc { get; set; }
}
