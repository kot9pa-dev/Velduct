using System.Threading.Channels;
using Velduct.Web.Messaging;

namespace Velduct.Web.Extensions;

public static class ChannelExtensions
{
    public static Stream AsObservableStream(this ChannelReader<ArchiveChunk> reader, ChannelWriter<byte[]> recyclePool)
    {
        return new ChannelStream(reader, recyclePool);
    }

    private class ChannelStream : Stream
    {
        private readonly ChannelReader<ArchiveChunk> _reader;
        private readonly ChannelWriter<byte[]> _recyclePool;
        private ArchiveChunk _currentBatch;
        private int _offset;

        public ChannelStream(ChannelReader<ArchiveChunk> reader, ChannelWriter<byte[]> recyclePool)
        {
            _reader = reader;
            _recyclePool = recyclePool;
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            // Fast-path: avoid await when data is buffered; TarReader calls this thousands of times per chunk
            if (_currentBatch.Data != null && _offset < _currentBatch.Length)
            {
                int toCopy = Math.Min(_currentBatch.Length - _offset, buffer.Length);
                _currentBatch.Data.AsMemory(_offset, toCopy).CopyTo(buffer);
                _offset += toCopy;
                return ValueTask.FromResult(toCopy);
            }

            // Slow-path: current chunk exhausted; wait for next from network
            return ReadNextChunkAsync(buffer, ct);
        }

        private async ValueTask<int> ReadNextChunkAsync(Memory<byte> buffer, CancellationToken ct)
        {
            if (_currentBatch.Data != null)
            {
                _recyclePool.TryWrite(_currentBatch.Data);
                _currentBatch = default;
            }

            if (!await _reader.WaitToReadAsync(ct)) return 0;
            if (!_reader.TryRead(out _currentBatch)) return 0;
            _offset = 0;

            int toCopy = Math.Min(_currentBatch.Length, buffer.Length);
            _currentBatch.Data.AsMemory(0, toCopy).CopyTo(buffer);
            _offset = toCopy;
            return toCopy;
        }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            // Return unread/stuck buffer back to pool
            if (_currentBatch.Data != null)
            {
                _recyclePool.TryWrite(_currentBatch.Data);
                _currentBatch = default;
            }
            base.Dispose(disposing);
        }
    }
}