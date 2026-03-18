namespace Velduct.Web.Services;

public interface IFrameHandler
{
    Task OnCreditsAsync(int count, CancellationToken ct);
    Task<bool> HasActiveArchiveAsync();
    Task OnArchiveChunkAsync(byte[] buffer, int written, CancellationToken ct);
    Task OnFullMessageAsync(byte opCode, byte[] payload, CancellationToken ct);
}
