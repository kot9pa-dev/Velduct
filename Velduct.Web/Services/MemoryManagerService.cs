using Velduct.Web.Configuration;
using Microsoft.Extensions.Options;
using System.Threading.Channels;

namespace Velduct.Web.Services
{
    public class MemoryManagerService
    {
        // Network buffer pool for receiving TAR from clients; buffer size matches ChunkSizeBytes
        public Channel<byte[]> NetworkBufferPool { get; }

        public MemoryManagerService(IOptions<TransferOptions> options)
        {
            var opts = options.Value;
            int networkPoolSize = opts.Network.NetworkPoolSize;
            int chunkSize = opts.Network.ChunkSizeBytes;

            NetworkBufferPool = Channel.CreateBounded<byte[]>(networkPoolSize);

            for (int i = 0; i < networkPoolSize; i++)
            {
                NetworkBufferPool.Writer.TryWrite(new byte[chunkSize]);
            }
        }
    }
}
