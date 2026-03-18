using System.Buffers.Binary;
using System.Text;
using Velduct.Web.Core;
using Xunit;

namespace Velduct.Web.Tests;

/// <summary>
/// Tests for Protocol.BuildFileMtimeAckMessage round-trip encoding/decoding.
/// Verifies that the binary message format is correctly built and can be parsed back.
/// </summary>
public class ProtocolTests
{
    /// <summary>
    /// Tests basic round-trip: build message, parse it back, verify all fields match.
    /// </summary>
    [Fact]
    public void BuildFileMtimeAckMessage_RoundTrip_BasicCase()
    {
        // Arrange
        string key = "myshare";
        string relPath = "documents/file.txt";
        long serverMtimeMs = 1710777600000; // arbitrary timestamp in ms

        // Act
        byte[] message = Protocol.BuildFileMtimeAckMessage(key, relPath, serverMtimeMs);

        // Assert
        Assert.NotNull(message);
        Assert.NotEmpty(message);

        // Parse the message back
        int pos = 0;
        byte opcode = message[pos++];
        Assert.Equal(Protocol.CMD_FILE_MTIME_ACK, opcode);

        int keyLen = BinaryPrimitives.ReadInt32LittleEndian(message.AsSpan(pos));
        pos += 4;
        string parsedKey = Encoding.UTF8.GetString(message, pos, keyLen);
        pos += keyLen;

        int pathLen = BinaryPrimitives.ReadInt32LittleEndian(message.AsSpan(pos));
        pos += 4;
        string parsedPath = Encoding.UTF8.GetString(message, pos, pathLen);
        pos += pathLen;

        long parsedMtime = BinaryPrimitives.ReadInt64LittleEndian(message.AsSpan(pos));

        // Verify fields match
        Assert.Equal(key, parsedKey);
        Assert.Equal(relPath, parsedPath);
        Assert.Equal(serverMtimeMs, parsedMtime);
    }

    /// <summary>
    /// Tests empty key string.
    /// </summary>
    [Fact]
    public void BuildFileMtimeAckMessage_RoundTrip_EmptyKey()
    {
        // Arrange
        string key = "";
        string relPath = "file.txt";
        long serverMtimeMs = 1710777600000;

        // Act
        byte[] message = Protocol.BuildFileMtimeAckMessage(key, relPath, serverMtimeMs);

        // Assert
        int pos = 0;
        byte opcode = message[pos++];
        Assert.Equal(Protocol.CMD_FILE_MTIME_ACK, opcode);

        int keyLen = BinaryPrimitives.ReadInt32LittleEndian(message.AsSpan(pos));
        pos += 4;
        Assert.Equal(0, keyLen);

        int pathLen = BinaryPrimitives.ReadInt32LittleEndian(message.AsSpan(pos));
        pos += 4;
        string parsedPath = Encoding.UTF8.GetString(message, pos, pathLen);
        pos += pathLen;

        long parsedMtime = BinaryPrimitives.ReadInt64LittleEndian(message.AsSpan(pos));

        Assert.Equal(relPath, parsedPath);
        Assert.Equal(serverMtimeMs, parsedMtime);
    }

    /// <summary>
    /// Tests empty path string.
    /// </summary>
    [Fact]
    public void BuildFileMtimeAckMessage_RoundTrip_EmptyPath()
    {
        // Arrange
        string key = "myshare";
        string relPath = "";
        long serverMtimeMs = 1710777600000;

        // Act
        byte[] message = Protocol.BuildFileMtimeAckMessage(key, relPath, serverMtimeMs);

        // Assert
        int pos = 0;
        byte opcode = message[pos++];
        Assert.Equal(Protocol.CMD_FILE_MTIME_ACK, opcode);

        int keyLen = BinaryPrimitives.ReadInt32LittleEndian(message.AsSpan(pos));
        pos += 4;
        string parsedKey = Encoding.UTF8.GetString(message, pos, keyLen);
        pos += keyLen;

        int pathLen = BinaryPrimitives.ReadInt32LittleEndian(message.AsSpan(pos));
        pos += 4;
        Assert.Equal(0, pathLen);

        long parsedMtime = BinaryPrimitives.ReadInt64LittleEndian(message.AsSpan(pos));

        Assert.Equal(key, parsedKey);
        Assert.Equal(serverMtimeMs, parsedMtime);
    }

    /// <summary>
    /// Tests Unicode characters in key and path.
    /// </summary>
    [Theory]
    [InlineData("share-日本語", "docs/ファイル.txt")]
    [InlineData("σηαρε", "αρχεία/ζIPfile.txt")]
    [InlineData("📁share", "📂📃file.txt")]
    public void BuildFileMtimeAckMessage_RoundTrip_UnicodeStrings(string key, string relPath)
    {
        // Arrange
        long serverMtimeMs = 1710777600000;

        // Act
        byte[] message = Protocol.BuildFileMtimeAckMessage(key, relPath, serverMtimeMs);

        // Assert
        int pos = 0;
        byte opcode = message[pos++];
        Assert.Equal(Protocol.CMD_FILE_MTIME_ACK, opcode);

        int keyLen = BinaryPrimitives.ReadInt32LittleEndian(message.AsSpan(pos));
        pos += 4;
        string parsedKey = Encoding.UTF8.GetString(message, pos, keyLen);
        pos += keyLen;

        int pathLen = BinaryPrimitives.ReadInt32LittleEndian(message.AsSpan(pos));
        pos += 4;
        string parsedPath = Encoding.UTF8.GetString(message, pos, pathLen);
        pos += pathLen;

        long parsedMtime = BinaryPrimitives.ReadInt64LittleEndian(message.AsSpan(pos));

        Assert.Equal(key, parsedKey);
        Assert.Equal(relPath, parsedPath);
        Assert.Equal(serverMtimeMs, parsedMtime);
    }

    /// <summary>
    /// Tests very long key and path strings.
    /// </summary>
    [Fact]
    public void BuildFileMtimeAckMessage_RoundTrip_LongStrings()
    {
        // Arrange
        string key = new string('a', 500);
        string relPath = new string('b', 1000) + "/" + new string('c', 500);
        long serverMtimeMs = 1710777600000;

        // Act
        byte[] message = Protocol.BuildFileMtimeAckMessage(key, relPath, serverMtimeMs);

        // Assert
        int pos = 0;
        byte opcode = message[pos++];
        Assert.Equal(Protocol.CMD_FILE_MTIME_ACK, opcode);

        int keyLen = BinaryPrimitives.ReadInt32LittleEndian(message.AsSpan(pos));
        pos += 4;
        string parsedKey = Encoding.UTF8.GetString(message, pos, keyLen);
        pos += keyLen;

        int pathLen = BinaryPrimitives.ReadInt32LittleEndian(message.AsSpan(pos));
        pos += 4;
        string parsedPath = Encoding.UTF8.GetString(message, pos, pathLen);
        pos += pathLen;

        long parsedMtime = BinaryPrimitives.ReadInt64LittleEndian(message.AsSpan(pos));

        Assert.Equal(key, parsedKey);
        Assert.Equal(relPath, parsedPath);
        Assert.Equal(serverMtimeMs, parsedMtime);
    }

    /// <summary>
    /// Tests various mtime values including edge cases (0, negative, max long).
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(1710777600000)]
    [InlineData(long.MaxValue)]
    public void BuildFileMtimeAckMessage_RoundTrip_VariousMtimeValues(long serverMtimeMs)
    {
        // Arrange
        string key = "test";
        string relPath = "file.txt";

        // Act
        byte[] message = Protocol.BuildFileMtimeAckMessage(key, relPath, serverMtimeMs);

        // Assert
        int pos = 0;
        byte opcode = message[pos++];
        Assert.Equal(Protocol.CMD_FILE_MTIME_ACK, opcode);

        int keyLen = BinaryPrimitives.ReadInt32LittleEndian(message.AsSpan(pos));
        pos += 4;
        pos += keyLen;

        int pathLen = BinaryPrimitives.ReadInt32LittleEndian(message.AsSpan(pos));
        pos += 4;
        pos += pathLen;

        long parsedMtime = BinaryPrimitives.ReadInt64LittleEndian(message.AsSpan(pos));

        Assert.Equal(serverMtimeMs, parsedMtime);
    }

    /// <summary>
    /// Tests message length calculation is correct.
    /// Format: [0x17][4-byte keyLen LE][key UTF8][4-byte pathLen LE][path UTF8][8-byte mtimeMs LE]
    /// </summary>
    [Fact]
    public void BuildFileMtimeAckMessage_CorrectMessageLength()
    {
        // Arrange
        string key = "myshare";
        string relPath = "documents/file.txt";
        long serverMtimeMs = 1710777600000;

        int expectedKeyByteLen = Encoding.UTF8.GetByteCount(key);
        int expectedPathByteLen = Encoding.UTF8.GetByteCount(relPath);
        int expectedTotalLength = 1 + 4 + expectedKeyByteLen + 4 + expectedPathByteLen + 8;

        // Act
        byte[] message = Protocol.BuildFileMtimeAckMessage(key, relPath, serverMtimeMs);

        // Assert
        Assert.Equal(expectedTotalLength, message.Length);
    }

    /// <summary>
    /// Tests that the opcode byte is always 0x17 (CMD_FILE_MTIME_ACK).
    /// </summary>
    [Fact]
    public void BuildFileMtimeAckMessage_OpcodeIsCorrect()
    {
        // Arrange
        string key = "test";
        string relPath = "file.txt";
        long serverMtimeMs = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();

        // Act
        byte[] message = Protocol.BuildFileMtimeAckMessage(key, relPath, serverMtimeMs);

        // Assert
        Assert.True(message.Length > 0);
        Assert.Equal(Protocol.CMD_FILE_MTIME_ACK, message[0]);
        Assert.Equal(0x17, message[0]);
    }

    /// <summary>
    /// Tests boundary case where key and path contain slashes and special characters.
    /// </summary>
    [Fact]
    public void BuildFileMtimeAckMessage_RoundTrip_SpecialCharacters()
    {
        // Arrange
        string key = "share/with/slashes";
        string relPath = "path/with spaces/and-dashes_underscore.txt";
        long serverMtimeMs = 1710777600000;

        // Act
        byte[] message = Protocol.BuildFileMtimeAckMessage(key, relPath, serverMtimeMs);

        // Assert
        int pos = 0;
        byte opcode = message[pos++];
        Assert.Equal(Protocol.CMD_FILE_MTIME_ACK, opcode);

        int keyLen = BinaryPrimitives.ReadInt32LittleEndian(message.AsSpan(pos));
        pos += 4;
        string parsedKey = Encoding.UTF8.GetString(message, pos, keyLen);
        pos += keyLen;

        int pathLen = BinaryPrimitives.ReadInt32LittleEndian(message.AsSpan(pos));
        pos += 4;
        string parsedPath = Encoding.UTF8.GetString(message, pos, pathLen);
        pos += pathLen;

        long parsedMtime = BinaryPrimitives.ReadInt64LittleEndian(message.AsSpan(pos));

        Assert.Equal(key, parsedKey);
        Assert.Equal(relPath, parsedPath);
        Assert.Equal(serverMtimeMs, parsedMtime);
    }

    // --- Extra protocol tests ---

    [Fact]
    public void BuildFileMtimeAckMessage_MtimeZero()
    {
        byte[] msg = Protocol.BuildFileMtimeAckMessage("k", "p", 0);
        int pos = 1 + 4 + 1 + 4 + 1; // opcode + keyLen + "k" + pathLen + "p"
        long mtime = BinaryPrimitives.ReadInt64LittleEndian(msg.AsSpan(pos));
        Assert.Equal(0L, mtime);
    }

    [Fact]
    public void BuildFileMtimeAckMessage_MtimeMaxValue()
    {
        long max = long.MaxValue;
        byte[] msg = Protocol.BuildFileMtimeAckMessage("k", "p", max);
        int pos = 1 + 4 + 1 + 4 + 1;
        long mtime = BinaryPrimitives.ReadInt64LittleEndian(msg.AsSpan(pos));
        Assert.Equal(max, mtime);
    }

    [Fact]
    public void BuildFileMtimeAckMessage_NegativeMtime()
    {
        long neg = -1710000000000; // pre-epoch
        byte[] msg = Protocol.BuildFileMtimeAckMessage("k", "p", neg);
        int pos = 1 + 4 + 1 + 4 + 1;
        long mtime = BinaryPrimitives.ReadInt64LittleEndian(msg.AsSpan(pos));
        Assert.Equal(neg, mtime);
    }

    [Fact]
    public void BuildFileMtimeAckMessage_ExactMessageLength()
    {
        string key = "myshare";
        string path = "docs/file.txt";
        byte[] msg = Protocol.BuildFileMtimeAckMessage(key, path, 12345);

        int expectedLen = 1 + 4 + Encoding.UTF8.GetByteCount(key) + 4 + Encoding.UTF8.GetByteCount(path) + 8;
        Assert.Equal(expectedLen, msg.Length);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(1710777600000)]
    [InlineData(long.MaxValue)]
    [InlineData(-1)]
    public void BuildFileMtimeAckMessage_RoundTrip_VariousMtimes(long mtimeMs)
    {
        byte[] msg = Protocol.BuildFileMtimeAckMessage("share", "file.txt", mtimeMs);

        int pos = 1;
        int kLen = BinaryPrimitives.ReadInt32LittleEndian(msg.AsSpan(pos)); pos += 4 + kLen;
        int pLen = BinaryPrimitives.ReadInt32LittleEndian(msg.AsSpan(pos)); pos += 4 + pLen;
        long parsed = BinaryPrimitives.ReadInt64LittleEndian(msg.AsSpan(pos));

        Assert.Equal(mtimeMs, parsed);
    }

    [Fact]
    public void BuildCheckFilesMessage_CarriesServerMtime()
    {
        var serverMtime = new DateTime(2026, 3, 18, 13, 25, 0, 123, DateTimeKind.Utc);
        long expectedMs = new DateTimeOffset(serverMtime).ToUnixTimeMilliseconds();

        var files = new List<Velduct.Web.Domain.FileMetadata>
        {
            new() { Key = "s", RelativePath = "f.txt", Size = 100, LastWriteTimeUtc = serverMtime }
        };

        byte[] msg = Protocol.BuildCheckFilesMessage(files);

        // Parse: [opcode][4-byte count][4-byte keyLen][key][4-byte pathLen][path][8-byte size][8-byte mtime]
        int pos = 1 + 4; // skip opcode + count
        int kLen = BinaryPrimitives.ReadInt32LittleEndian(msg.AsSpan(pos)); pos += 4 + kLen;
        int pLen = BinaryPrimitives.ReadInt32LittleEndian(msg.AsSpan(pos)); pos += 4 + pLen;
        pos += 8; // skip size
        long parsedMs = BinaryPrimitives.ReadInt64LittleEndian(msg.AsSpan(pos));

        Assert.Equal(expectedMs, parsedMs);
    }
}
