using System.Buffers.Binary;
using System.Text;
using Velduct.Web.Domain;

namespace Velduct.Web.Core;

public static class Protocol
{
    public const byte CMD_PULL_FILE = 0x10;
    public const byte CMD_OFFER_FILE = 0x11;
    public const byte CMD_SKIP_FILE = 0x12;
    public const byte CMD_CHECK_FILES = 0x13;
    public const byte CMD_BATCH_PULL = 0x14;
    public const byte CMD_DELETE_CONFIRM = 0x15;
    public const byte CMD_REGISTER_SHARES = 0x16;
    public const byte CMD_FILE_MTIME_ACK = 0x17;

    public const byte CMD_FILE_DATA = 0x06;
    public const byte CMD_FILE_DONE = 0x07;

    public const byte SRV_PULL_STREAM = 0x20;
    public const byte SRV_DONE = 0x12;

    public const byte CMD_ARCHIVE_RAW_START = 0x30;
    public const byte CMD_ARCHIVE_DATA = 0x31;
    public const byte CMD_ARCHIVE_DONE = 0x32;

    public static readonly byte[] CreditMessage = { SRV_PULL_STREAM };
    public static readonly byte[] ArchiveStartMessage = { CMD_ARCHIVE_RAW_START };
    public static readonly byte[] ArchiveDoneMessage = { CMD_ARCHIVE_DONE };

    public static byte[] BuildCreditMessage(int count)
    {
        if (count <= 1) return CreditMessage;
        return new byte[] { SRV_PULL_STREAM, (byte)count };
    }

    // Single allocation; write directly without MemoryStream
    public static byte[] BuildCheckFilesMessage(IReadOnlyList<FileMetadata> files)
    {
        int totalSize = 1 + 4;
        foreach (var f in files)
        {
            totalSize += 4 + Encoding.UTF8.GetByteCount(f.Key)
                       + 4 + Encoding.UTF8.GetByteCount(f.RelativePath)
                       + 8 + 8;
        }

        byte[] buf = new byte[totalSize];
        int pos = 0;

        buf[pos++] = CMD_CHECK_FILES;
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), files.Count);
        pos += 4;

        foreach (var f in files)
        {
            int keyLen = Encoding.UTF8.GetBytes(f.Key, buf.AsSpan(pos + 4));
            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), keyLen);
            pos += 4 + keyLen;

            int pathLen = Encoding.UTF8.GetBytes(f.RelativePath, buf.AsSpan(pos + 4));
            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), pathLen);
            pos += 4 + pathLen;

            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), f.Size);
            pos += 8;

            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos),
                new DateTimeOffset(f.LastWriteTimeUtc).ToUnixTimeMilliseconds());
            pos += 8;
        }

        return buf;
    }

    public static byte[] BuildBatchPullMessage(IReadOnlyList<FileMetadata> files)
    {
        int totalSize = 1 + 4;
        foreach (var f in files)
        {
            totalSize += 4 + Encoding.UTF8.GetByteCount(f.Key)
                       + 4 + Encoding.UTF8.GetByteCount(f.RelativePath)
                       + 8 + 8;
        }

        byte[] buf = new byte[totalSize];
        int pos = 0;

        buf[pos++] = CMD_BATCH_PULL;
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), files.Count);
        pos += 4;

        foreach (var f in files)
        {
            int keyLen = Encoding.UTF8.GetBytes(f.Key, buf.AsSpan(pos + 4));
            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), keyLen);
            pos += 4 + keyLen;

            int pathLen = Encoding.UTF8.GetBytes(f.RelativePath, buf.AsSpan(pos + 4));
            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), pathLen);
            pos += 4 + pathLen;

            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), f.Size);
            pos += 8;

            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos),
                new DateTimeOffset(f.LastWriteTimeUtc).ToUnixTimeMilliseconds());
            pos += 8;
        }

        return buf;
    }

    // Single allocation; zero-copy write of server mtime ACK for uploading client
    public static byte[] BuildFileMtimeAckMessage(string key, string relPath, long serverMtimeMs)
    {
        int keyByteLen = Encoding.UTF8.GetByteCount(key);
        int pathByteLen = Encoding.UTF8.GetByteCount(relPath);
        int totalSize = 1 + 4 + keyByteLen + 4 + pathByteLen + 8;

        byte[] buf = new byte[totalSize];
        int pos = 0;

        buf[pos++] = CMD_FILE_MTIME_ACK;
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), keyByteLen);
        pos += 4;
        Encoding.UTF8.GetBytes(key, buf.AsSpan(pos));
        pos += keyByteLen;
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), pathByteLen);
        pos += 4;
        Encoding.UTF8.GetBytes(relPath, buf.AsSpan(pos));
        pos += pathByteLen;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), serverMtimeMs);

        return buf;
    }

    public static byte[] BuildDeleteConfirmMessage(IReadOnlyList<string> paths)
    {
        int totalSize = 1 + 4;
        foreach (var p in paths)
        {
            totalSize += 4 + Encoding.UTF8.GetByteCount(p);
        }

        byte[] buf = new byte[totalSize];
        int pos = 0;

        buf[pos++] = CMD_DELETE_CONFIRM;
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), paths.Count);
        pos += 4;

        foreach (var p in paths)
        {
            int len = Encoding.UTF8.GetBytes(p, buf.AsSpan(pos + 4));
            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), len);
            pos += 4 + len;
        }

        return buf;
    }
}
