package transfer

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"Velduct.GoClient/internal/protocol"
)

// fileSender coordinates file upload workers: single-file (offer→chunks→done) and batch (TAR).
// Both use muTask for sequential socket writes.
// Atomicity: files copied to .ddzs_temp/ with metadata verification before send.
type fileSender struct{
	s *Session // доступ к общим полям сессии
}

func newFileSender(s *Session) *fileSender {
	return &fileSender{s: s}
}

// --- Single-file worker ---

func (fs *fileSender) runSingleUploadWorker() {
	defer fs.s.wg.Done()
	slog.Debug("[Sender] Single upload worker started")

	for {
		select {
		case <-fs.s.ctx.Done():
			slog.Debug("[Sender] Single upload worker stopped")
			return
		case task := <-fs.s.singleQueue:
			fs.s.muTask.Lock()
			fs.processSingleUpload(task)
			fs.s.muTask.Unlock()
		}
	}
}

func (fs *fileSender) processSingleUpload(task uploadTask) {
	baseDir, ok := fs.s.Shares[task.key]
	if !ok {
		slog.Warn("[Sender] Unknown share key", "key", task.key)
		return
	}

	fullPath := filepath.Join(baseDir, task.relPath)

	// Проверяем что файл существует и не директория
	info, err := os.Stat(fullPath)
	if err != nil {
		slog.Debug("[Sender] File not found or stat failed", "path", fullPath, "err", err)
		return
	}
	if info.IsDir() {
		slog.Debug("[Sender] Path is a directory, skipping", "path", fullPath)
		return
	}

	tempDir := ResolveTempDir(fs.s.Config.TempDirs, task.key)
	buf := <-fs.s.uploadBufPool
	result, copyErr := CopyToTemp(fs.s.ctx, fullPath, tempDir, buf, nil, fs.s.Config.MaxCopyRetries, fs.s.Config.FileLockRetryDelayMs)
	fs.s.uploadBufPool <- buf

	if copyErr != nil {
		slog.Warn("[Sender] Failed to create verified temp copy, skipping",
			"key", task.key, "path", task.relPath, "err", copyErr)
		return
	}
	defer os.Remove(result.TempPath)

	fs.s.SendOffer(task.key, task.relPath, result.Size, result.MTimeMs)

	if err := fs.uploadFile(result.TempPath); err != nil {
		slog.Error("[Sender] Single upload failed",
			"key", task.key, "path", task.relPath, "err", err)
	}
}

func (fs *fileSender) uploadFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := <-fs.s.uploadBufPool
	defer func() { fs.s.uploadBufPool <- buf }()

	chunksSent := 0
	for {
		n, err := f.Read(buf)
		if n > 0 {
			select {
			case <-fs.s.credits:
			case <-fs.s.ctx.Done():
				slog.Debug("[Sender] Upload aborted by context cancellation", "path", path)
				return io.ErrUnexpectedEOF
			case <-time.After(time.Duration(fs.s.Config.SingleUploadTimeoutSec) * time.Second):
				slog.Error("[Sender] Credit timeout — server not responding",
					"path", path,
					"timeout_sec", fs.s.Config.SingleUploadTimeoutSec,
					"chunks_sent", chunksSent)
				return io.ErrUnexpectedEOF
			}

			payload := make([]byte, n+1)
			payload[0] = protocol.CmdFileData
			copy(payload[1:], buf[:n])
			if sendErr := fs.s.safeSend(payload); sendErr != nil {
				return sendErr
			}
			chunksSent++
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	slog.Debug("[Sender] Single file uploaded", "path", path, "chunks", chunksSent)
	return fs.s.safeSend([]byte{protocol.CmdFileDone})
}

// --- Batch worker ---

func (fs *fileSender) runBatchWorker() {
	defer fs.s.wg.Done()
	slog.Debug("[Sender] Batch worker started")

	maxArchiveSizeBytes := int64(fs.s.Config.MaxCredits) * int64(fs.s.Config.ChunkSizeBytes)

	for {
		select {
		case <-fs.s.ctx.Done():
			slog.Debug("[Sender] Batch worker stopped")
			return
		case batch := <-fs.s.batchQueue:
			currentSize := calcBatchSize(batch)

		drainLoop:
			for len(batch) < (fs.s.Config.ScanBatchSize*fs.s.Config.MaxCredits) && currentSize < maxArchiveSizeBytes {
				select {
				case <-fs.s.ctx.Done():
					return
				case more := <-fs.s.batchQueue:
					batch = append(batch, more...)
					currentSize += calcBatchSize(more)
					if currentSize >= maxArchiveSizeBytes || len(batch) >= (fs.s.Config.ScanBatchSize*fs.s.Config.MaxCredits) {
						break drainLoop
					}
				default:
					break drainLoop
				}
			}

			fs.s.muTask.Lock()
			slog.Debug("[Sender] Processing merged batch",
				"total_files", len(batch),
				"size_mb", currentSize/(1024*1024),
				"remaining_in_queue", len(fs.s.batchQueue))

			if err := fs.s.tar.SendFileList(fs.s.Shares, batch); err != nil {
				slog.Error("[Sender] Batch TAR error", "err", err)
			}
			fs.s.muTask.Unlock()
		}
	}
}

func calcBatchSize(files []ArchiveFile) int64 {
	var total int64
	for _, f := range files {
		total += f.Size
	}
	return total
}
