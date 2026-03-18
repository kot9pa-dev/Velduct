#nullable enable
using System.Net.WebSockets;
using System.Threading.Channels;

namespace Velduct.Web.Messaging;

// Wraps an outbound WebSocket message; returns buffer to pool after send if ReturnPool is set
public readonly struct OutboundMessage
{
    public byte[] Data { get; }

    // Actual length of data; 0 means use Data.Length (for pooled buffers)
    public int Length { get; }

    public WebSocketMessageType Type { get; }
    public bool EndOfMessage { get; }

    // Pool to return Data to after send; null = standard GC
    public Channel<byte[]>? ReturnPool { get; }

    public OutboundMessage(byte[] data, WebSocketMessageType type, bool endOfMessage)
    {
        Data = data;
        Length = 0;
        Type = type;
        EndOfMessage = endOfMessage;
        ReturnPool = null;
    }

    public OutboundMessage(byte[] data, int length, WebSocketMessageType type, bool endOfMessage, Channel<byte[]>? returnPool)
    {
        Data = data;
        Length = length;
        Type = type;
        EndOfMessage = endOfMessage;
        ReturnPool = returnPool;
    }

    public ReadOnlyMemory<byte> AsMemory() =>
        Length > 0 ? Data.AsMemory(0, Length) : Data.AsMemory();
}
