package transfer

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"Velduct.GoClient/internal/config"
	"Velduct.GoClient/internal/protocol"
	gosync "Velduct.GoClient/internal/sync"
)

// Session coordinates all file transfer components for one connection.
// Routes incoming commands to receiver/sender. Business logic: FileReceiver (receive), fileSender (send).
type Session struct {
	Config   config.Config
	Shares   map[string]string
	SendFunc func([]byte) error

	muSend        sync.Mutex
	muTask        sync.Mutex
	credits       chan struct{}
	uploadBufPool chan []byte

	tar             *TarUploader
	archiveReceiver *ArchiveReceiver
	syncHandler     *gosync.SyncHandler

	batchQueue  chan []ArchiveFile
	singleQueue chan uploadTask

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	cancelledPaths sync.Map

	// recentDownloads stores serverMtime and actualFsMtime after Chtimes.
	// Used for anti-echo and to report server's mtime to avoid false change detection.
	recentDownloads sync.Map

	downloads *DownloadTracker

	receiver *FileReceiver
	sender   *fileSender
}

type uploadTask struct {
	key     string
	relPath string
}

func NewSession(shares map[string]string, sendFunc func([]byte) error, cfg config.Config) *Session {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Session{
		Config:        cfg,
		Shares:        shares,
		SendFunc:      sendFunc,
		credits:       make(chan struct{}, cfg.MaxCredits),
		uploadBufPool: make(chan []byte, cfg.MaxCredits),
		batchQueue:    make(chan []ArchiveFile, cfg.MaxCredits),
		singleQueue:   make(chan uploadTask, cfg.UploadQueueSize),
		ctx:           ctx,
		cancel:        cancel,
	}

	s.downloads = NewDownloadTracker()
	s.tar = NewTarUploader(s.safeSend, cfg, &s.cancelledPaths)
	s.receiver = newFileReceiver(shares, cfg.TempDirs, s.downloads, s.safeSend, cfg.MaxCredits, s.StoreDownloadedMtime)
	s.archiveReceiver = newArchiveReceiver(shares, cfg.TempDirs, s.downloads, s.safeSend, cfg.MaxCredits, s.StoreDownloadedMtime)
	s.syncHandler = gosync.NewSyncHandler(shares, s.safeSend, s.downloads.CancelIfActive, cfg.ScanBatchSize)
	s.sender = newFileSender(s)

	for i := 0; i < cfg.MaxCredits; i++ {
		s.uploadBufPool <- make([]byte, cfg.ChunkSizeBytes)
	}

	s.wg.Add(2)
	go s.sender.runBatchWorker()
	go s.sender.runSingleUploadWorker()

	slog.Debug("[Session] Created", "max_credits", cfg.MaxCredits, "chunk_kb", cfg.ChunkSizeBytes/1024)
	return s
}

// Close terminates session and waits for all workers to stop.
func (s *Session) Close() {
	slog.Debug("[Session] Closing, waiting for workers to stop...")
	s.cancel()

	if s.archiveReceiver.IsActive() {
		s.archiveReceiver.HandleArchiveDone()
	}

	s.receiver.CloseAndCleanup()

	s.wg.Wait()
	slog.Debug("[Session] All workers stopped.")
}

// --- Shared ---

func (s *Session) safeSend(payload []byte) error {
	s.muSend.Lock()
	defer s.muSend.Unlock()
	return s.SendFunc(payload)
}

// AddCredit routes credit to appropriate channel. TAR uploader has priority.
func (s *Session) AddCredit() {
	select {
	case s.tar.Credits <- struct{}{}:
	default:
		select {
		case s.credits <- struct{}{}:
		default:
			slog.Debug("[Session] Credit dropped — both channels full")
		}
	}
}

// HandlePull handles server request for single file.
func (s *Session) HandlePull(payload []byte) {
	if len(payload) < 9 {
		slog.Warn("[Session] HandlePull: payload too short", "len", len(payload))
		return
	}
	buf := bytes.NewReader(payload[1:])
	var kLen, pLen int32
	binary.Read(buf, binary.LittleEndian, &kLen)
	k := make([]byte, kLen)
	buf.Read(k)
	binary.Read(buf, binary.LittleEndian, &pLen)
	p := make([]byte, pLen)
	buf.Read(p)

	archiveName := string(k) + "/" + filepath.ToSlash(string(p))
	s.clearCancelledPath(archiveName)

	select {
	case s.singleQueue <- uploadTask{key: string(k), relPath: string(p)}:
		slog.Debug("[Session] Single upload enqueued", "key", string(k), "path", string(p))
	default:
		slog.Warn("[Session] Single upload queue full, dropping task",
			"key", string(k), "path", string(p),
			"queue_capacity", s.Config.UploadQueueSize)
	}
}

// HandleBatchPull handles server request for batch of files (forward sync).
func (s *Session) HandleBatchPull(payload []byte) {
	if len(payload) < 5 {
		slog.Warn("[Session] HandleBatchPull: payload too short", "len", len(payload))
		return
	}

	files := decodeBatchPayload(payload)
	if len(files) == 0 {
		slog.Warn("[Session] HandleBatchPull: decoded 0 files from payload")
		return
	}

	for _, f := range files {
		s.clearCancelledPath(f.Key + "/" + filepath.ToSlash(f.RelPath))
	}

	select {
	case s.batchQueue <- files:
		slog.Debug("[Session] Batch enqueued", "files", len(files), "queue_depth", len(s.batchQueue))
	case <-s.ctx.Done():
		slog.Debug("[Session] Batch dropped — session closing")
	}
}

// HandleServerCheckFiles handles CmdCheckFiles from server (reverse sync).
func (s *Session) HandleServerCheckFiles(payload []byte) {
	s.syncHandler.HandleServerCheckFiles(payload)
}

// HandleDeleteConfirm registers deleted paths so sender can abort transmission.
func (s *Session) HandleDeleteConfirm(payload []byte) {
	if len(payload) < 5 {
		slog.Warn("[Session] HandleDeleteConfirm: payload too short", "len", len(payload))
		return
	}
	buf := bytes.NewReader(payload[1:])
	var count int32
	if binary.Read(buf, binary.LittleEndian, &count) != nil || count <= 0 {
		slog.Warn("[Session] HandleDeleteConfirm: invalid count in payload")
		return
	}

	cancelled := 0
	for i := 0; i < int(count); i++ {
		var pLen int32
		if binary.Read(buf, binary.LittleEndian, &pLen) != nil {
			slog.Warn("[Session] HandleDeleteConfirm: truncated payload at index", "i", i)
			break
		}
		pathBytes := make([]byte, pLen)
		buf.Read(pathBytes)
		s.cancelledPaths.Store(string(pathBytes), struct{}{})
		cancelled++
	}

	slog.Debug("[Session] Delete confirmed from server", "cancelled_uploads", cancelled)
}

func (s *Session) HandleOffer(payload []byte) {
	s.receiver.HandleOffer(payload, s.Shares)
}

func (s *Session) HandleData(data []byte) {
	s.receiver.HandleData(data)
}

func (s *Session) HandleDone() {
	s.receiver.HandleDone(s.Shares)
}

func (s *Session) HandleArchiveStart() {
	s.archiveReceiver.HandleArchiveStart()
}

func (s *Session) HandleArchiveData(data []byte) {
	s.archiveReceiver.HandleArchiveData(data)
}

func (s *Session) HandleArchiveDone() {
	s.archiveReceiver.HandleArchiveDone()
}

// SendAllSharesAsTar initiates full upload of all shares as single TAR stream.
func (s *Session) SendAllSharesAsTar() error {
	return s.tar.SendSharesAsTar(s.Shares)
}

// CancelActiveDownload cancels active file download if in progress.
// Returns true if download was found and marked.
func (s *Session) CancelActiveDownload(finalPath string) bool {
	return s.downloads.CancelIfActive(finalPath)
}

func (s *Session) IsCancelled(keyRelPath string) bool {
	_, ok := s.cancelledPaths.Load(keyRelPath)
	return ok
}

func (s *Session) clearCancelledPath(archiveName string) {
	current := archiveName
	for current != "" && current != "." && current != "/" {
		s.cancelledPaths.Delete(current)
		dir := filepath.ToSlash(filepath.Dir(current))
		if dir == current {
			break
		}
		current = dir
	}
}

type downloadedMtime struct {
	ServerMtimeMs   int64
	ActualFsMtimeMs int64
}

// StoreDownloadedMtime saves both mtimes after successful download.
func (s *Session) StoreDownloadedMtime(fullPath string, serverMs, actualFsMs int64) {
	s.recentDownloads.Store(fullPath, downloadedMtime{
		ServerMtimeMs:   serverMs,
		ActualFsMtimeMs: actualFsMs,
	})
}

// IsRecentDownload checks if file was recently downloaded and unchanged since.
// Returns true if current FS mtime matches stored actual FS mtime (no modification).
func (s *Session) IsRecentDownload(fullPath string, mtimeMs int64) bool {
	v, ok := s.recentDownloads.Load(fullPath)
	if !ok {
		return false
	}
	dm := v.(downloadedMtime)
	return dm.ActualFsMtimeMs == mtimeMs
}

// ResolveReportMtime returns mtime to report to server.
// If file unchanged since download: return server's mtime to avoid false change detection.
// If file was modified locally and client clock is behind server: bump to serverMtime+1
// to guarantee the server recognizes the edit as newer than the cached version.
func (s *Session) ResolveReportMtime(fullPath string, currentFsMtimeMs int64) int64 {
	v, ok := s.recentDownloads.Load(fullPath)
	if !ok {
		return currentFsMtimeMs
	}
	dm := v.(downloadedMtime)
	if dm.ActualFsMtimeMs == currentFsMtimeMs {
		return dm.ServerMtimeMs // Unchanged since download
	}
	// File was modified locally. Ensure reported mtime > server cached version.
	if currentFsMtimeMs > dm.ServerMtimeMs {
		return currentFsMtimeMs
	}
	return dm.ServerMtimeMs + 1 // Client clock behind server — bump to ensure pull
}

func (s *Session) SendPull(key, path string) {
	buf := new(bytes.Buffer)
	buf.WriteByte(protocol.CmdPullFile)
	binary.Write(buf, binary.LittleEndian, int32(len(key)))
	buf.WriteString(key)
	binary.Write(buf, binary.LittleEndian, int32(len(path)))
	buf.WriteString(path)
	s.safeSend(buf.Bytes())
}

func (s *Session) SendOffer(key, path string, size, mtime int64) {
	buf := new(bytes.Buffer)
	buf.WriteByte(protocol.CmdOfferFile)
	binary.Write(buf, binary.LittleEndian, int32(len(key)))
	buf.WriteString(key)
	binary.Write(buf, binary.LittleEndian, int32(len(path)))
	buf.WriteString(path)
	binary.Write(buf, binary.LittleEndian, size)
	binary.Write(buf, binary.LittleEndian, mtime)
	s.safeSend(buf.Bytes())
}

// HandleFileMtimeAck processes server-authoritative mtime ACK after successful upload.
// Sets the server timestamp on the local file so all clients share the same mtime.
// Registers in recentDownloads for anti-echo (watcher must not re-upload after Chtimes).
func (s *Session) HandleFileMtimeAck(payload []byte) {
	// payload (opcode already stripped): [4-byte keyLen][key][4-byte pathLen][path][8-byte mtimeMs]
	if len(payload) < 4 {
		return
	}
	pos := 0
	kLen := int(int32(payload[pos]) | int32(payload[pos+1])<<8 | int32(payload[pos+2])<<16 | int32(payload[pos+3])<<24)
	pos += 4
	if pos+kLen > len(payload) {
		return
	}
	key := string(payload[pos : pos+kLen])
	pos += kLen

	if pos+4 > len(payload) {
		return
	}
	pLen := int(int32(payload[pos]) | int32(payload[pos+1])<<8 | int32(payload[pos+2])<<16 | int32(payload[pos+3])<<24)
	pos += 4
	if pos+pLen > len(payload) {
		return
	}
	relPath := string(payload[pos : pos+pLen])
	pos += pLen

	if pos+8 > len(payload) {
		return
	}
	mtimeMs := int64(payload[pos]) | int64(payload[pos+1])<<8 | int64(payload[pos+2])<<16 | int64(payload[pos+3])<<24 |
		int64(payload[pos+4])<<32 | int64(payload[pos+5])<<40 | int64(payload[pos+6])<<48 | int64(payload[pos+7])<<56

	baseDir, ok := s.Shares[key]
	if !ok {
		return
	}

	fullPath := filepath.Join(baseDir, filepath.FromSlash(relPath))

	// Set server-authoritative mtime on local file
	t := time.Unix(0, mtimeMs*int64(time.Millisecond))
	if err := os.Chtimes(fullPath, t, t); err != nil {
		slog.Debug("[Session] Failed to set server mtime from ACK", "path", fullPath, "err", err)
		return
	}

	// Store for anti-echo: next watcher trigger must not re-upload
	if fi, err := os.Stat(fullPath); err == nil {
		s.StoreDownloadedMtime(fullPath, mtimeMs, fi.ModTime().UnixNano()/1e6)
	}

	slog.Debug("[Session] Server mtime set from ACK", "key", key, "path", relPath, "mtimeMs", mtimeMs)
}

func decodeBatchPayload(payload []byte) []ArchiveFile {
	buf := bytes.NewReader(payload[1:])
	var count int32
	if err := binary.Read(buf, binary.LittleEndian, &count); err != nil || count <= 0 {
		return nil
	}

	files := make([]ArchiveFile, 0, count)
	for i := 0; i < int(count); i++ {
		var kLen, pLen int32
		if binary.Read(buf, binary.LittleEndian, &kLen) != nil {
			break
		}
		k := make([]byte, kLen)
		buf.Read(k)
		if binary.Read(buf, binary.LittleEndian, &pLen) != nil {
			break
		}
		p := make([]byte, pLen)
		buf.Read(p)

		var size, mtime int64
		if binary.Read(buf, binary.LittleEndian, &size) != nil {
			break
		}
		if binary.Read(buf, binary.LittleEndian, &mtime) != nil {
			break
		}

		files = append(files, ArchiveFile{Key: string(k), RelPath: string(p), Size: size, MTime: mtime})
	}
	return files
}
