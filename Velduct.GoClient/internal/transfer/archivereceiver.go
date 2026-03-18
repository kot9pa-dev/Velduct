package transfer

import (
	"archive/tar"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"Velduct.GoClient/internal/protocol"
)

// ArchiveReceiver receives TAR stream from server and unpacks to disk.
// Lifecycle: CmdArchiveStart → N×CmdArchiveData → CmdArchiveDone.
// Each file: write to .ddzs_temp/ → atomic rename to share.
type ArchiveReceiver struct {
	shares     map[string]string
	tempDirs   map[string]string // per-share temp directories (из Config.TempDirs)
	downloads  *DownloadTracker  // трекер активных загрузок для отмены при delete/rename
	storeMtime func(fullPath string, serverMs, actualFsMs int64) // callback для сохранения mtime
	sendFunc   func([]byte) error
	credits    int

	mu     sync.Mutex
	active bool
	dataCh chan []byte
	doneCh chan struct{}
}

func newArchiveReceiver(shares map[string]string, tempDirs map[string]string, downloads *DownloadTracker, sendFunc func([]byte) error, credits int, storeMtime func(string, int64, int64)) *ArchiveReceiver {
	return &ArchiveReceiver{
		shares:     shares,
		tempDirs:   tempDirs,
		downloads:  downloads,
		storeMtime: storeMtime,
		sendFunc:   sendFunc,
		credits:    credits,
	}
}

// HandleArchiveStart initializes receiving new TAR stream.
func (ar *ArchiveReceiver) HandleArchiveStart() {
	ar.mu.Lock()
	defer ar.mu.Unlock()

	if ar.active {
		slog.Info("[ArchiveReceiver] New archive started while previous is still active, closing old.")
		if ar.dataCh != nil {
			close(ar.dataCh)
		}
	}

	ar.dataCh = make(chan []byte, ar.credits)
	ar.doneCh = make(chan struct{})
	ar.active = true

	// Issue initial credits async to avoid deadlock during concurrent TAR upload.
	go func() {
		if ar.credits > 1 {
			ar.sendFunc([]byte{protocol.SrvPullStream, byte(ar.credits)})
		} else {
			ar.sendFunc([]byte{protocol.SrvPullStream})
		}
	}()

	go ar.unpackLoop()

	slog.Debug("[ArchiveReceiver] Archive reception started, issued initial credits", "credits", ar.credits)
}

// HandleArchiveData processes incoming TAR data chunk.
func (ar *ArchiveReceiver) HandleArchiveData(data []byte) {
	ar.mu.Lock()
	if !ar.active || ar.dataCh == nil {
		ar.mu.Unlock()
		slog.Debug("[ArchiveReceiver] Data received but no active archive, ignoring")
		return
	}
	ch := ar.dataCh
	ar.mu.Unlock()

	// Copy data: payload may be reused by caller
	buf := make([]byte, len(data))
	copy(buf, data)

	ch <- buf

	// Issue credit async to avoid blocking read loop
	go ar.sendFunc([]byte{protocol.SrvPullStream})
}

// HandleArchiveDone completes TAR stream reception.
func (ar *ArchiveReceiver) HandleArchiveDone() {
	ar.mu.Lock()
	if !ar.active {
		ar.mu.Unlock()
		return
	}

	ch := ar.dataCh
	done := ar.doneCh
	ar.dataCh = nil
	ar.active = false
	ar.mu.Unlock()

	if ch != nil {
		close(ch)
	}

	// Ждём завершения распаковки
	if done != nil {
		<-done
	}

	slog.Debug("[ArchiveReceiver] Archive reception complete")
}

// IsActive checks if archive reception is active.
func (ar *ArchiveReceiver) IsActive() bool {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	return ar.active
}

// unpackLoop reads chunks, forms TAR stream, and unpacks files.
func (ar *ArchiveReceiver) unpackLoop() {
	defer close(ar.doneCh)

	reader := &channelReader{ch: ar.dataCh}
	tr := tar.NewReader(reader)

	processed := 0
	skipped := 0
	var totalBytes int64

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			slog.Warn("[ArchiveReceiver] TAR read error", "err", err)
			break
		}

		if header.Typeflag != tar.TypeReg {
			continue
		}

		parts := strings.SplitN(header.Name, "/", 2)
		if len(parts) < 2 || parts[1] == "" {
			slog.Debug("[ArchiveReceiver] Skipping entry with unexpected name", "name", header.Name)
			skipped++
			continue
		}

		key := parts[0]
		relPath := parts[1]

		baseDir, ok := ar.shares[key]
		if !ok {
			slog.Warn("[ArchiveReceiver] Unknown share key in TAR entry, skipping", "key", key)
			skipped++
			continue
		}

		if err := ar.extractEntry(tr, header, baseDir, key, relPath); err != nil {
			slog.Error("[ArchiveReceiver] Failed to extract entry", "key", key, "path", relPath, "err", err)
			continue
		}
		totalBytes += header.Size
		processed++
	}

	slog.Info("[ArchiveReceiver] Unpack complete",
		"files", processed,
		"skipped", skipped,
		"sizeMB", float64(totalBytes)/(1024*1024))
}

// extractEntry extracts one file from TAR via .ddzs_temp/ and atomically renames to share.
func (ar *ArchiveReceiver) extractEntry(tr *tar.Reader, header *tar.Header, baseDir, key, relPath string) error {
	fullPath := filepath.Join(baseDir, filepath.FromSlash(relPath))

	// Path traversal protection
	absBase, _ := filepath.Abs(baseDir)
	absFull, _ := filepath.Abs(fullPath)
	if !isSubPath(absBase, absFull) {
		slog.Warn("[ArchiveReceiver] Path traversal blocked", "key", key, "path", relPath)
		return nil
	}

	// Write to temp dir on same filesystem for atomic rename
	tempDir := ResolveTempDir(ar.tempDirs, key)
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return err
	}

	tempFile, err := os.CreateTemp(tempDir, "dl-*.tmp")
	if err != nil {
		return err
	}

	ar.downloads.Register(fullPath, tempFile.Name())

	_, err = io.Copy(tempFile, tr)
	tempFile.Close()

	if err != nil {
		os.Remove(tempFile.Name())
		ar.downloads.Unregister(fullPath)
		return err
	}

	if ar.downloads.IsCancelled(fullPath) {
		slog.Debug("[ArchiveReceiver] Download cancelled during extract, discarding",
			"key", key, "path", relPath)
		os.Remove(tempFile.Name())
		ar.downloads.Unregister(fullPath)
		return nil
	}

	// Создаём целевую директорию
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		os.Remove(tempFile.Name())
		ar.downloads.Unregister(fullPath)
		return err
	}

	// Retry rename indefinitely while temp file exists: on Windows, antivirus/indexer/explorer
	// may briefly lock the target. Consistent with server-side DiskWorkerService retry policy.
	for attempt := 0; ; attempt++ {
		err = os.Rename(tempFile.Name(), fullPath)
		if err == nil {
			break
		}
		// If temp file was removed externally, abort
		if _, statErr := os.Stat(tempFile.Name()); statErr != nil {
			ar.downloads.Unregister(fullPath)
			return err
		}
		if attempt == 0 {
			slog.Debug("[ArchiveReceiver] Rename locked, retrying", "path", relPath, "err", err)
		} else if attempt%20 == 0 {
			slog.Warn("[ArchiveReceiver] Still waiting to rename", "path", relPath, "attempts", attempt)
		}
		time.Sleep(500 * time.Millisecond)
	}

	ar.downloads.Unregister(fullPath)

	mtime := header.ModTime
	if !mtime.IsZero() {
		os.Chtimes(fullPath, mtime, mtime)
	}

	// Store server mtime + actual FS mtime after Chtimes to report server's mtime and avoid echo.
	serverMtimeMs := mtime.UnixNano() / 1e6
	if fi, err := os.Stat(fullPath); err == nil {
		ar.storeMtime(fullPath, serverMtimeMs, fi.ModTime().UnixNano()/1e6)
	}

	slog.Debug("[ArchiveReceiver] File extracted", "key", key, "path", relPath, "size", header.Size)
	return nil
}

// channelReader adapts chan []byte to io.Reader for tar.NewReader.
type channelReader struct {
	ch      chan []byte
	current []byte
	offset  int
}

func (cr *channelReader) Read(p []byte) (int, error) {
	for {
		if cr.offset < len(cr.current) {
			n := copy(p, cr.current[cr.offset:])
			cr.offset += n
			return n, nil
		}

		data, ok := <-cr.ch
		if !ok {
			return 0, io.EOF
		}
		cr.current = data
		cr.offset = 0
	}
}
