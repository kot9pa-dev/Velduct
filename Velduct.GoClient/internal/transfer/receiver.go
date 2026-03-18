package transfer

import (
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"Velduct.GoClient/internal/protocol"
)

// FileReceiver manages file reception from server (push mode).
// Lifecycle: HandleOffer → N×HandleData → HandleDone.
// File: write to .ddzs_temp/ → atomic rename to share.
type FileReceiver struct{
	shares        map[string]string
	tempDirs      map[string]string
	downloads     *DownloadTracker
	storeMtime    func(fullPath string, serverMs, actualFsMs int64)
	sendFunc      func([]byte) error
	credits       int
	fsPrecisionMs int64

	mu           sync.Mutex
	isReceiving  bool
	file         *os.File
	finalPath    string
	relPath      string
	key          string
	expectedSize int64
	expectedTime int64
}

func newFileReceiver(shares map[string]string, tempDirs map[string]string, downloads *DownloadTracker, sendFunc func([]byte) error, credits int, storeMtime func(string, int64, int64), fsPrecisionMs int64) *FileReceiver {
	return &FileReceiver{
		shares:        shares,
		tempDirs:      tempDirs,
		downloads:     downloads,
		storeMtime:    storeMtime,
		sendFunc:      sendFunc,
		credits:       credits,
		fsPrecisionMs: fsPrecisionMs,
	}
}

// HandleOffer handles server's file offer. Opens temp file and issues credits for chunks.
func (r *FileReceiver) HandleOffer(payload []byte, shares map[string]string) {
	key, relPath, size, mtime, ok := decodeOfferPayload(payload)
	if !ok {
		slog.Warn("[Receiver] HandleOffer: failed to decode payload", "len", len(payload))
		return
	}

	baseDir, exists := shares[key]
	if !exists {
		slog.Warn("[Receiver] HandleOffer: unknown share key", "key", key)
		return
	}

	fullPath := filepath.Join(baseDir, relPath)

	// Path traversal protection
	absBase, _ := filepath.Abs(baseDir)
	absFull, _ := filepath.Abs(fullPath)
	if !isSubPath(absBase, absFull) {
		slog.Warn("[Receiver] Path traversal blocked", "key", key, "path", relPath)
		return
	}

	if info, err := os.Stat(fullPath); err == nil {
		localMs := info.ModTime().UnixNano() / 1e6
		if info.Size() == size && truncateMtimeMs(localMs, r.fsPrecisionMs) == truncateMtimeMs(mtime, r.fsPrecisionMs) {
			slog.Debug("[Receiver] File already up-to-date, skipping", "key", key, "path", relPath)
			go r.sendFunc([]byte{protocol.CmdSkipFile})
			return
		}
	}

	tempDir := ResolveTempDir(r.tempDirs, key)
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		slog.Error("[Receiver] Failed to create temp dir",
			"path", tempDir, "err", err)
		return
	}

	f, err := os.CreateTemp(tempDir, "dl-*.tmp")
	if err != nil {
		slog.Error("[Receiver] Failed to create temp file",
			"dir", tempDir, "err", err)
		return
	}

	r.mu.Lock()
	r.isReceiving = true
	r.file = f
	r.finalPath = fullPath
	r.relPath = relPath
	r.key = key
	r.expectedSize = size
	r.expectedTime = mtime
	r.mu.Unlock()

	r.downloads.Register(fullPath, f.Name())

	go func() {
		for i := 0; i < r.credits; i++ {
			r.sendFunc([]byte{protocol.SrvPullStream})
		}
	}()

	slog.Debug("[Receiver] Offer accepted", "key", key, "path", relPath, "size", size)
}

// HandleData writes chunk to file and issues credit for next chunk.
func (r *FileReceiver) HandleData(data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.isReceiving || r.file == nil {
		slog.Debug("[Receiver] HandleData: not receiving, ignoring chunk")
		return
	}

	if _, err := r.file.Write(data); err != nil {
		slog.Error("[Receiver] Write failed, aborting transfer", "path", r.relPath, "err", err)
		// Abort: close file, mark not receiving. HandleDone will see isReceiving=false.
		tempPath := r.file.Name()
		r.file.Close()
		r.file = nil
		r.isReceiving = false
		os.Remove(tempPath)
		r.downloads.Unregister(r.finalPath)
		return
	}
	go r.sendFunc([]byte{protocol.SrvPullStream})
}

// HandleDone finalizes reception: closes file, verifies size, and atomically renames temp→final.
func (r *FileReceiver) HandleDone(shares map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.isReceiving || r.file == nil {
		slog.Debug("[Receiver] HandleDone: not receiving, ignoring")
		return
	}

	tempPath := r.file.Name()
	finalPath := r.finalPath
	r.file.Close()
	r.file = nil
	r.isReceiving = false

	if r.downloads.IsCancelled(finalPath) {
		slog.Debug("[Receiver] Download cancelled (file deleted/renamed during transfer), discarding",
			"key", r.key, "path", r.relPath)
		os.Remove(tempPath)
		r.downloads.Unregister(finalPath)
		return
	}

	info, err := os.Stat(tempPath)
	if err != nil {
		slog.Error("[Receiver] Temp file not found after receive",
			"temp_path", tempPath, "err", err)
		r.downloads.Unregister(finalPath)
		return
	}

	if info.Size() != r.expectedSize {
		slog.Warn("[Receiver] Size mismatch, discarding",
			"path", finalPath,
			"expected", r.expectedSize,
			"got", info.Size())
		os.Remove(tempPath)
		r.downloads.Unregister(finalPath)
		return
	}

	if err := os.MkdirAll(filepath.Dir(finalPath), 0755); err != nil {
		slog.Error("[Receiver] Failed to create target dir",
			"path", filepath.Dir(finalPath), "err", err)
		os.Remove(tempPath)
		r.downloads.Unregister(finalPath)
		return
	}

	if err := os.Rename(tempPath, finalPath); err != nil {
		slog.Error("[Receiver] Rename failed",
			"from", tempPath, "to", finalPath, "err", err)
		os.Remove(tempPath)
		r.downloads.Unregister(finalPath)
		return
	}

	r.downloads.Unregister(finalPath)

	t := time.Unix(0, r.expectedTime*1e6)
	if err := os.Chtimes(finalPath, t, t); err != nil {
		slog.Debug("[Receiver] Failed to set mtime", "path", finalPath, "err", err)
	}

	// Store server mtime + actual FS mtime after Chtimes to report server's mtime and avoid echo.
	if fi, err := os.Stat(finalPath); err == nil {
		r.storeMtime(finalPath, r.expectedTime, fi.ModTime().UnixNano()/1e6)
	}

	slog.Debug("[Receiver] File received ok", "path", finalPath, "size", r.expectedSize)
}

// CloseAndCleanup closes current file and deletes temp to avoid orphaned files on session close.
func (r *FileReceiver) CloseAndCleanup() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.isReceiving || r.file == nil {
		return
	}

	tempPath := r.file.Name()
	finalPath := r.finalPath
	r.file.Close()
	r.file = nil
	r.isReceiving = false

	r.downloads.Unregister(finalPath)

	if err := os.Remove(tempPath); err != nil && !os.IsNotExist(err) {
		slog.Debug("[Receiver] Failed to cleanup temp file on session close",
			"path", tempPath, "err", err)
	} else {
		slog.Debug("[Receiver] Cleaned up temp file on session close", "path", tempPath)
	}
}

// --- Helpers ---

func isSubPath(base, target string) bool {
	base = filepath.Clean(base) + string(filepath.Separator)
	target = filepath.Clean(target) + string(filepath.Separator)
	return len(target) >= len(base) && target[:len(base)] == base
}

func truncateMtimeMs(ms, precisionMs int64) int64 {
	if precisionMs <= 1 {
		return ms
	}
	if ms >= 0 {
		return (ms / precisionMs) * precisionMs
	}
	return ((ms - precisionMs + 1) / precisionMs) * precisionMs
}

func decodeOfferPayload(payload []byte) (key, relPath string, size, mtime int64, ok bool) {
	if len(payload) < 9 {
		return "", "", 0, 0, false
	}

	pos := 0
	if len(payload) < pos+4 {
		return "", "", 0, 0, false
	}
	kLen := int(int32(payload[pos]) | int32(payload[pos+1])<<8 | int32(payload[pos+2])<<16 | int32(payload[pos+3])<<24)
	pos += 4

	if len(payload) < pos+kLen {
		return "", "", 0, 0, false
	}
	key = string(payload[pos : pos+kLen])
	pos += kLen

	if len(payload) < pos+4 {
		return "", "", 0, 0, false
	}
	pLen := int(int32(payload[pos]) | int32(payload[pos+1])<<8 | int32(payload[pos+2])<<16 | int32(payload[pos+3])<<24)
	pos += 4

	if len(payload) < pos+pLen {
		return "", "", 0, 0, false
	}
	relPath = string(payload[pos : pos+pLen])
	pos += pLen

	if len(payload) < pos+16 {
		return "", "", 0, 0, false
	}
	size = int64(payload[pos]) | int64(payload[pos+1])<<8 | int64(payload[pos+2])<<16 | int64(payload[pos+3])<<24 |
		int64(payload[pos+4])<<32 | int64(payload[pos+5])<<40 | int64(payload[pos+6])<<48 | int64(payload[pos+7])<<56
	pos += 8
	mtime = int64(payload[pos]) | int64(payload[pos+1])<<8 | int64(payload[pos+2])<<16 | int64(payload[pos+3])<<24 |
		int64(payload[pos+4])<<32 | int64(payload[pos+5])<<40 | int64(payload[pos+6])<<48 | int64(payload[pos+7])<<56

	return key, relPath, size, mtime, true
}
