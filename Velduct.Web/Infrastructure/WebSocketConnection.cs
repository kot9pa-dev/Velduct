using System.Net.WebSockets;
using System.Threading.Channels;
using Velduct.Web.Messaging;

namespace Velduct.Web.Infrastructure;

public class WebSocketConnection
{
    public Guid Id { get; } = Guid.NewGuid();

    public WebSocket Socket { get; }

    // Outbound message queue; SendLoop drains continuously to socket
    public Channel<OutboundMessage> SendChannel { get; }

    // Flow control credits; each incoming SRV_PULL_STREAM byte grants one chunk send
    public Channel<byte> OutboundCredits { get; }

    // Async mutex over socket writes; SendLoop holds per-message, ServerArchiveSender holds for entire TAR stream
    public Channel<bool> SendPermit { get; } = Channel.CreateBounded<bool>(1);

    public WebSocketConnection(WebSocket socket, int maxCredits, int sendChannelCapacity)
    {
        Socket = socket;
        OutboundCredits = Channel.CreateBounded<byte>(maxCredits);
        SendChannel = Channel.CreateBounded<OutboundMessage>(
            new BoundedChannelOptions(sendChannelCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true
            });

        SendPermit.Writer.TryWrite(true);
    }

    public void EnqueueSend(byte[] buffer, WebSocketMessageType type, bool endOfMessage)
    {
        if (!SendChannel.Writer.TryWrite(new OutboundMessage(buffer, type, endOfMessage)))
        {
            // Bounded channel full (256 capacity) — message dropped.
            // Should not happen: SendLoop drains continuously, DrainSendChannelAsync runs during TAR sends.
            System.Diagnostics.Debug.WriteLine(
                $"[WebSocketConnection] SendChannel full, message dropped (opcode=0x{buffer[0]:X2}, len={buffer.Length})");
        }
    }
}
