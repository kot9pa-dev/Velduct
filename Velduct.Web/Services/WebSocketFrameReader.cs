using System.Net.WebSockets;
using Velduct.Web.Configuration;
using Velduct.Web.Core;
using Microsoft.Extensions.Options;

namespace Velduct.Web.Services;

// Reads WebSocket frames and classifies them; archive chunks go directly to pool buffers without copy
public sealed class WebSocketFrameReader
{
    private readonly ILogger<WebSocketFrameReader> _logger;
    private readonly MemoryManagerService _memory;
    private readonly TransferOptions _options;

    public WebSocketFrameReader(ILogger<WebSocketFrameReader> logger, MemoryManagerService memory, IOptions<TransferOptions> options)
    {
        _logger = logger;
        _memory = memory;
        _options = options.Value;
    }

    public async Task ReadLoopAsync(
        WebSocket webSocket,
        IFrameHandler handler,
        CancellationToken ct)
    {
        byte[] headerBuffer = new byte[_options.Network.HeaderBufferSize];
        byte[] messagePayload = new byte[_options.Network.MessagePayloadBufferSize]; // covers max non-archive message
        int messagePayloadLen = 0;
        byte currentOpCode = 0;
        bool isNewMessage = true;

        byte[] activeChunkBuffer = null;
        int activeChunkWritten = 0;

        try
        {
            while (webSocket.State == WebSocketState.Open && !ct.IsCancellationRequested)
            {
                ValueWebSocketReceiveResult result;

                if (activeChunkBuffer != null)
                {
                    result = await webSocket.ReceiveAsync(
                        activeChunkBuffer.AsMemory(activeChunkWritten), ct);
                }
                else
                {
                    result = await webSocket.ReceiveAsync(headerBuffer.AsMemory(), ct);
                }

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    _logger.LogInformation("WebSocket close frame received from client.");
                    break;
                }

                if (result.MessageType != WebSocketMessageType.Binary)
                    continue;

                int offset = 0;
                int count = result.Count;

                if (isNewMessage && count > 0)
                {
                    currentOpCode = headerBuffer[0];

                    if (currentOpCode == Protocol.SRV_PULL_STREAM)
                    {
                        int creditCount = (count > 1) ? headerBuffer[1] : 1;
                        if (creditCount <= 0) creditCount = 1;
                        await handler.OnCreditsAsync(creditCount, ct);
                        isNewMessage = true;
                        continue;
                    }

                    offset = 1;
                    count -= 1;
                    messagePayloadLen = 0;

                    if (currentOpCode == Protocol.CMD_ARCHIVE_DATA && count > 0)
                    {
                        bool hasActiveArchive = await handler.HasActiveArchiveAsync();
                        if (hasActiveArchive)
                        {
                            activeChunkBuffer = await _memory.NetworkBufferPool.Reader.ReadAsync(ct);
                            Buffer.BlockCopy(headerBuffer, offset, activeChunkBuffer, 0, count);
                            activeChunkWritten = count;

                            if (result.EndOfMessage)
                            {
                                var buf = activeChunkBuffer;
                                var written = activeChunkWritten;
                                activeChunkBuffer = null;
                                activeChunkWritten = 0;
                                await handler.OnArchiveChunkAsync(buf, written, ct);
                                isNewMessage = true;
                            }
                            else
                            {
                                isNewMessage = false;
                            }
                            continue;
                        }
                        // Нет активного архива — падаем в обычный путь (messageBuffer)
                    }
                }
                else if (activeChunkBuffer != null)
                {
                    // Продолжение архивного чанка — данные уже пишутся прямо в пул-буфер
                    activeChunkWritten += count;

                    if (result.EndOfMessage)
                    {
                        var buf = activeChunkBuffer;
                        var written = activeChunkWritten;
                        activeChunkBuffer = null;
                        activeChunkWritten = 0;
                        await handler.OnArchiveChunkAsync(buf, written, ct);
                        isNewMessage = true;
                    }
                    continue;
                }

                if (count > 0)
                {
                    Buffer.BlockCopy(headerBuffer, offset, messagePayload, messagePayloadLen, count);
                    messagePayloadLen += count;
                }

                if (result.EndOfMessage)
                {
                    isNewMessage = true;
                    await handler.OnFullMessageAsync(currentOpCode, messagePayload.AsSpan(0, messagePayloadLen).ToArray(), ct);
                }
                else
                {
                    isNewMessage = false;
                }
            }
        }
        finally
        {
            if (activeChunkBuffer != null)
            {
                _logger.LogInformation("Connection dropped with active chunk buffer in-flight, returning to pool.");
                _memory.NetworkBufferPool.Writer.TryWrite(activeChunkBuffer);
            }
        }
    }
}