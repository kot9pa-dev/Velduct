using System.Net.WebSockets;
using System.Threading.Channels;
using Velduct.Web.Messaging;

namespace Velduct.Web.Infrastructure;

public class WebSocketConnection
{
    public Guid Id { get; } = Guid.NewGuid();

    public WebSocket Socket { get; }

    // Outbound message queue; SendLoop drains continuously to socket.
    // Unbounded: EnqueueSend never blocks, never drops.
    // Memory bounded by CreditTimeoutSeconds — stalled TAR send aborts, SendPermit released, queue drains.
    public Channel<OutboundMessage> SendChannel { get; }

    // Flow control credits; each incoming SRV_PULL_STREAM byte grants one chunk send
    public Channel<byte> OutboundCredits { get; }

    // Async mutex over socket writes; SendLoop holds per-message, ServerArchiveSender holds for entire TAR stream
    public Channel<bool> SendPermit { get; } = Channel.CreateBounded<bool>(1);

    public WebSocketConnection(WebSocket socket, int maxCredits, int sendChannelCapacity)
    {
        Socket = socket;
        OutboundCredits = Channel.CreateBounded<byte>(maxCredits);

        SendChannel = Channel.CreateUnbounded<OutboundMessage>(new UnboundedChannelOptions
        {
            SingleReader = true
        });

        SendPermit.Writer.TryWrite(true);
    }

    public void EnqueueSend(byte[] buffer, WebSocketMessageType type, bool endOfMessage)
    {
        if (Socket.State != WebSocketState.Open)
            return;

        SendChannel.Writer.TryWrite(new OutboundMessage(buffer, type, endOfMessage));
    }
}
