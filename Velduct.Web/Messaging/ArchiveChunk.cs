namespace Velduct.Web.Messaging;

public readonly struct ArchiveChunk
{
    public byte[] Data { get; }
    public int Length { get; }

    public ArchiveChunk(byte[] data, int length)
    {
        Data = data;
        Length = length;
    }
}
