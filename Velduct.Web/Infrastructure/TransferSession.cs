using System.Threading.Channels;
using Velduct.Web.Messaging;

namespace Velduct.Web.Infrastructure;

public class TransferSession
{
    public WebSocketConnection Connection { get; }

    // Bounded to provide backpressure to the disk subsystem
    public Channel<FileDiskTask> CreateTasks { get; }

    public Channel<FileDiskTask> FinalizeTasks { get; }

    public Channel<byte[]> DataChannel { get; } = Channel.CreateUnbounded<byte[]>();

    // Capacity mirrors MaxCredits to prevent network from outpacing processing
    public Channel<ArchiveChunk> ArchiveChannel { get; set; }

    // Background TAR unpacking task; awaited on restart to recover buffers to pool
    public Task UnpackTask { get; set; }

    // CancellationTokenSource to abort UnpackTask on archive restart
    public CancellationTokenSource UnpackCts { get; set; }

    public TransferSession(WebSocketConnection connection, int maxCredits, int createTasksCapacity, int finalizeTasksCapacity)
    {
        Connection = connection;
        CreateTasks = Channel.CreateBounded<FileDiskTask>(createTasksCapacity);
        FinalizeTasks = Channel.CreateBounded<FileDiskTask>(finalizeTasksCapacity);
    }
}