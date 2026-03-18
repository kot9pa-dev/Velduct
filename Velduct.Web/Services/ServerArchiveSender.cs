using System.Formats.Tar;
using System.IO.Pipelines;
using System.Net.WebSockets;
using System.Threading.Channels;
using Velduct.Web.Configuration;
using Velduct.Web.Core;
using Velduct.Web.Domain;
using Velduct.Web.Infrastructure;
using Microsoft.Extensions.Options;

namespace Velduct.Web.Services;

// Sends files as TAR stream; holds SendPermit for entire stream to avoid WebSocket contention
public sealed class ServerArchiveSender
{
    private readonly ILogger<ServerArchiveSender> _logger;
    private readonly TransferOptions _options;

    // Per-connection buffer pool; avoids contention on global channel when sending to multiple clients
    private readonly Channel<byte[]> _chunkPool;

    public ServerArchiveSender(
        ILogger<ServerArchiveSender> logger,
        IOptions<TransferOptions> options)
    {
        _logger = logger;
        _options = options.Value;

        int poolSize = _options.Network.MaxCredits;
        _chunkPool = Channel.CreateBounded<byte[]>(poolSize);
        for (int i = 0; i < poolSize; i++)
        {
            _chunkPool.Writer.TryWrite(new byte[_options.Network.ChunkSizeBytes + 1]);
        }
    }

    public async Task SendFilesToClientAsync(
        WebSocketConnection connection,
        IReadOnlyList<FileMetadata> requestedFiles,
        CancellationToken ct)
    {
        if (requestedFiles.Count == 0) return;

        _logger.LogDebug("Starting TAR send to client: {Count} files requested.", requestedFiles.Count);

        int drained = 0;
        while (connection.OutboundCredits.Reader.TryRead(out _))
            drained++;
        if (drained > 0)
            _logger.LogDebug("Drained {Count} leftover credits before TAR send.", drained);

        // Захватываем эксклюзивное право на WebSocket
        await connection.SendPermit.Reader.ReadAsync(ct);

        int filesSent = 0;
        int filesSkipped = 0;
        long totalBytes = 0;

        try
        {
            // 1. CmdArchiveStart — напрямую в сокет
            await connection.Socket.SendAsync(
                Protocol.ArchiveStartMessage,
                WebSocketMessageType.Binary, true, ct);

            var pipe = new Pipe(new PipeOptions(
                minimumSegmentSize: _options.Network.ChunkSizeBytes,
                pauseWriterThreshold: _options.Network.ChunkSizeBytes * 8,
                resumeWriterThreshold: _options.Network.ChunkSizeBytes * 4));

            // Producer: формирует TAR и пишет в pipe
            var producerTask = Task.Run(async () =>
            {
                try
                {
                    await using var tarWriter = new TarWriter(pipe.Writer.AsStream(leaveOpen: true));

                    foreach (var file in requestedFiles)
                    {
                        if (ct.IsCancellationRequested) break;

                        string filePath = Path.Combine(_options.Storage.DataDirectory, file.Key, file.RelativePath);
                        if (!File.Exists(filePath))
                        {
                            filesSkipped++;
                            continue;
                        }

                        try
                        {
                            // File.GetLastWriteTimeUtc — прямой syscall без аллокации FileInfo объекта
                            var lastWrite = File.GetLastWriteTimeUtc(filePath);
                            string archiveName = file.Key + "/" + file.RelativePath;

                            FileStream dataStream = null;
                            int retries = 0;
                            while (!ct.IsCancellationRequested)
                            {
                                try
                                {
                                    // Буфер 4096 — минимальный, т.к. Pipe уже буферизует (pauseWriterThreshold).
                                    // IoBufferSize (256KB+) попадает на LOH → 101 инстанс = 26MB мёртвой памяти до GC2.
                                    dataStream = new FileStream(filePath,
                                        FileMode.Open, FileAccess.Read,
                                        FileShare.ReadWrite | FileShare.Delete,
                                        4096, FileOptions.Asynchronous | FileOptions.SequentialScan);
                                    break;
                                }
                                catch (IOException)
                                {
                                    if (!File.Exists(filePath))
                                        break;

                                    retries++;
                                    if (retries == 1)
                                        _logger.LogDebug("File locked, waiting: {Key}/{Path}", file.Key, file.RelativePath);
                                    else if (retries % 20 == 0)
                                        _logger.LogWarning("Still waiting for file lock: {Key}/{Path} ({Retries} retries)",
                                            file.Key, file.RelativePath, retries);

                                    await Task.Delay(_options.Network.FileLockRetryDelayMs, ct);
                                }
                            }

                            if (dataStream == null)
                            {
                                filesSkipped++;
                                continue;
                            }

                            long fileLen;
                            using (dataStream)
                            {
                                fileLen = dataStream.Length;
                                var entry = new PaxTarEntry(TarEntryType.RegularFile, archiveName)
                                {
                                    DataStream = dataStream,
                                    ModificationTime = lastWrite
                                };

                                await tarWriter.WriteEntryAsync(entry, ct);
                            }
                            totalBytes += fileLen;
                            filesSent++;
                        }
                        catch (Exception ex)
                        {
                            _logger.LogInformation(ex, "Failed to add {Key}/{Path} to TAR, skipping.",
                                file.Key, file.RelativePath);
                            filesSkipped++;
                        }
                    }
                }
                catch (OperationCanceledException) { }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "TAR producer error.");
                }
                finally
                {
                    await pipe.Writer.CompleteAsync();
                }
            }, ct);

            // Consumer: читает из pipe чанками, ждёт кредитов, отправляет напрямую
            await SendChunkedFromPipeAsync(connection, pipe.Reader, ct);
            await producerTask;

            // 3. CmdArchiveDone — напрямую в сокет
            if (connection.Socket.State == WebSocketState.Open)
            {
                await connection.Socket.SendAsync(
                    Protocol.ArchiveDoneMessage,
                    WebSocketMessageType.Binary, true, ct);
            }

            // Финальный drain: отправляем всё что накопилось за время TAR-стрима
            // (кредиты, CmdCheckFiles и т.д.) ДО освобождения SendPermit.
            await DrainSendChannelAsync(connection, ct);
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("TAR send cancelled.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during TAR send to client.");
        }
        finally
        {
            // Финальный drain перед освобождением SendPermit
            // (на случай если drain выше был пропущен из-за исключения)
            try { await DrainSendChannelAsync(connection, CancellationToken.None); }
            catch { /* ignore — мы в finally */ }

            // Всегда освобождаем SendPermit
            connection.SendPermit.Writer.TryWrite(true);
        }

        _logger.LogInformation("TAR send complete: {Sent} files, {Skipped} skipped, {SizeMB:F2} MB.",
            filesSent, filesSkipped, totalBytes / (1024.0 * 1024.0));
    }

    /// <summary>
    /// Читает данные из PipeReader, нарезает на чанки с префиксом CmdArchiveData
    /// и отправляет НАПРЯМУЮ в WebSocket (SendPermit уже захвачен).
    ///
    /// Буферы берутся из per-connection _chunkPool и возвращаются сразу после отправки.
    /// Без промежуточных каналов, без per-chunk CancellationTokenSource.
    ///
    /// ВАЖНО: Пока мы держим SendPermit, SendLoop заблокирован и не может отправлять
    /// сообщения из SendChannel. Поэтому между чанками дренируем SendChannel и отправляем
    /// накопившиеся сообщения сами — иначе кредиты для клиентской загрузки (от ArchiveProcessor)
    /// застрянут в очереди, и клиент получит credit timeout.
    /// </summary>
    private async Task SendChunkedFromPipeAsync(
        WebSocketConnection connection,
        PipeReader reader,
        CancellationToken ct)
    {
        int chunkSize = _options.Network.ChunkSizeBytes;
        int creditTimeoutMs = _options.Network.CreditTimeoutSeconds * 1000;
        var pool = _chunkPool;

        // Один CTS на весь стрим — переиспользуем через TryReset().
        // Вместо Task.WhenAny + Task.Delay + .AsTask() (4-5 аллокаций на чанк)
        // тратим 0 аллокаций когда TryRead срабатывает, и 0 дополнительных когда нет
        // (CTS уже создан, CancelAfter переиспользует внутренний Timer).
        using var creditCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        try
        {
            while (true)
            {
                var readResult = await reader.ReadAsync(ct);
                var buffer = readResult.Buffer;

                if (buffer.IsEmpty && readResult.IsCompleted)
                    break;

                while (buffer.Length > 0)
                {
                    // Дренируем SendChannel — отправляем накопившиеся сообщения
                    // (кредиты для клиентской загрузки, CmdCheckFiles, и т.д.)
                    await DrainSendChannelAsync(connection, ct);

                    // Ждём кредит от клиента — zero-alloc path когда TryRead работает
                    if (!connection.OutboundCredits.Reader.TryRead(out _))
                    {
                        creditCts.CancelAfter(creditTimeoutMs);

                        try
                        {
                            bool available = await connection.OutboundCredits.Reader.WaitToReadAsync(creditCts.Token);
                            if (!available || !connection.OutboundCredits.Reader.TryRead(out _))
                            {
                                reader.AdvanceTo(buffer.Start);
                                return;
                            }
                        }
                        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                        {
                            _logger.LogWarning("Credit timeout from client during server TAR send.");
                            reader.AdvanceTo(buffer.Start);
                            return;
                        }

                        // Сбрасываем таймер для следующего чанка
                        if (!creditCts.TryReset())
                            return; // parent ct отменён
                    }

                    // Берём буфер из пула (блокируется если все in-flight)
                    byte[] sendBuf = await pool.Reader.ReadAsync(ct);

                    int takeBytes = (int)Math.Min(buffer.Length, chunkSize);
                    sendBuf[0] = Protocol.CMD_ARCHIVE_DATA;

                    var slice = buffer.Slice(0, takeBytes);
                    int destOffset = 1;
                    foreach (var segment in slice)
                    {
                        segment.Span.CopyTo(sendBuf.AsSpan(destOffset));
                        destOffset += segment.Length;
                    }
                    buffer = buffer.Slice(takeBytes);

                    // Отправляем напрямую в сокет (SendPermit уже наш)
                    try
                    {
                        await connection.Socket.SendAsync(
                            sendBuf.AsMemory(0, 1 + takeBytes),
                            WebSocketMessageType.Binary, true, ct);
                    }
                    finally
                    {
                        pool.Writer.TryWrite(sendBuf);
                    }
                }

                reader.AdvanceTo(buffer.Start, buffer.End);

                if (readResult.IsCompleted)
                    break;
            }
        }
        finally
        {
            await reader.CompleteAsync();
        }
    }

    /// <summary>
    /// Дренирует и отправляет все накопившиеся сообщения из SendChannel.
    /// Вызывается только когда SendPermit захвачен (мы единственный писатель в сокет).
    /// Критически важно для кредитов: ArchiveProcessor.FlushChunkAsync() ставит
    /// SRV_PULL_STREAM в SendChannel, но SendLoop не может их отправить пока мы
    /// держим SendPermit → клиент не получает кредиты → credit timeout.
    /// </summary>
    private async Task DrainSendChannelAsync(WebSocketConnection connection, CancellationToken ct)
    {
        while (connection.SendChannel.Reader.TryRead(out var msg))
        {
            if (connection.Socket.State != WebSocketState.Open)
            {
                msg.ReturnPool?.Writer.TryWrite(msg.Data);
                return;
            }

            try
            {
                await connection.Socket.SendAsync(msg.AsMemory(), msg.Type, msg.EndOfMessage, ct);
            }
            catch (WebSocketException ex)
            {
                _logger.LogDebug("DrainSendChannel: WebSocket send failed: {Message}", ex.Message);
            }
            finally
            {
                msg.ReturnPool?.Writer.TryWrite(msg.Data);
            }
        }
    }
}
