package transfer

import (
	"archive/tar"
	"context"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"Velduct.GoClient/internal/config"
	"Velduct.GoClient/internal/constants"
	"Velduct.GoClient/internal/protocol"
)

// ArchiveFile describes a file to include in archive.
type ArchiveFile struct{
	Key     string
	RelPath string
	Size    int64
	MTime   int64
}

// TarUploader implements streaming TAR archive upload.
// Files: copy from share to .ddzs_temp/ with verification → pack from stable copy.
type TarUploader struct {
	Config   config.Config
	sendFunc func([]byte) error
	muSend   sync.Mutex

	Credits  chan struct{}
	bufPool  chan []byte
	copyPool chan []byte

	cancelledPaths *sync.Map
}

func NewTarUploader(sendFunc func([]byte) error, cfg config.Config, cancelledPaths *sync.Map) *TarUploader {
	u := &TarUploader{
		Config:         cfg,
		sendFunc:       sendFunc,
		Credits:        make(chan struct{}, cfg.MaxCredits),
		bufPool:        make(chan []byte, cfg.MaxCredits),
		copyPool:       make(chan []byte, cfg.MaxCredits),
		cancelledPaths: cancelledPaths,
	}

	if cfg.MaxCredits < 3 {
		slog.Error("[TarUploader] MaxCredits must be >= 3 to avoid deadlock in appendFileToTar (double buffer acquisition)",
			"current", cfg.MaxCredits)
	}

	for i := 0; i < cfg.MaxCredits; i++ {
		u.bufPool <- make([]byte, cfg.ChunkSizeBytes+1)
		u.copyPool <- make([]byte, cfg.ChunkSizeBytes)
	}

	return u
}

func (u *TarUploader) safeSend(data []byte) error {
	u.muSend.Lock()
	defer u.muSend.Unlock()
	return u.sendFunc(data)
}

// SendSharesAsTar scans all shares and streams as single TAR to server.
func (u *TarUploader) SendSharesAsTar(shares map[string]string) error {
	slog.Info("[TarUploader] Starting full shares TAR upload", "shares", len(shares))

	return u.streamTar(context.Background(), func(tw *tar.Writer) (int, int64, error) {
		var files int
		var totalBytes int64
		for key, baseDir := range shares {
			f, b, err := u.walkShare(tw, key, baseDir)
			if err != nil {
				return files, totalBytes, err
			}
			files += f
			totalBytes += b
		}
		return files, totalBytes, nil
	})
}

// SendFileList streams specific file list as TAR to server.
func (u *TarUploader) SendFileList(shares map[string]string, files []ArchiveFile) error {
	return u.SendFileListCtx(context.Background(), shares, files)
}

// SendFileListCtx streams file list with context support.
func (u *TarUploader) SendFileListCtx(ctx context.Context, shares map[string]string, files []ArchiveFile) error {
	slog.Debug("[TarUploader] Starting file list TAR upload", "files", len(files))

	return u.streamTar(ctx, func(tw *tar.Writer) (int, int64, error) {
		var sent int
		var totalBytes int64
		skipped := 0
		for _, f := range files {
			baseDir, ok := shares[f.Key]
			if !ok {
				slog.Warn("[TarUploader] Share not found for file, skipping", "key", f.Key, "path", f.RelPath)
				skipped++
				continue
			}

			absPath := filepath.Join(baseDir, filepath.FromSlash(f.RelPath))
			archiveName := f.Key + "/" + filepath.ToSlash(f.RelPath)

			tempDir := ResolveTempDir(u.Config.TempDirs, f.Key)
			if err := u.appendFileToTar(ctx, tw, absPath, archiveName, tempDir); err != nil {
				return sent, totalBytes, err
			}
			sent++
			totalBytes += f.Size
		}
		if skipped > 0 {
			slog.Warn("[TarUploader] Skipped files due to missing shares", "count", skipped)
		}
		return sent, totalBytes, nil
	})
}

// streamTar is the core pipeline (Producer-Consumer).
func (u *TarUploader) streamTar(ctx context.Context, packData func(tw *tar.Writer) (int, int64, error)) error {
	if err := u.safeSend([]byte{protocol.CmdArchiveStart}); err != nil {
		return err
	}

	sendQueue := make(chan []byte, u.Config.MaxCredits)

	cw := &tarChunkWriter{
		pool:      u.bufPool,
		sendQueue: sendQueue,
	}
	tw := tar.NewWriter(cw)

	pumpDone := make(chan error, 1)
	go func() {
		pumpDone <- u.runSendPump(ctx, sendQueue)
	}()

	files, totalBytes, writeErr := packData(tw)

	if writeErr == nil {
		writeErr = tw.Close()
	}

	cw.flush()
	close(sendQueue)

	if err := <-pumpDone; err != nil {
		return err
	}

	if err := u.safeSend([]byte{protocol.CmdArchiveDone}); err != nil {
		return err
	}

	slog.Info("[TarUploader] TAR upload complete",
		"files", files,
		"sizeMB", float64(totalBytes)/(1024*1024))
	return writeErr
}

// tarChunkWriter slices tar.Writer stream into chunks for pipeline.
type tarChunkWriter struct {
	pool      chan []byte
	sendQueue chan []byte
	buf       []byte
	offset    int
}

func (cw *tarChunkWriter) Write(p []byte) (int, error) {
	written := 0
	for written < len(p) {
		if cw.buf == nil {
			cw.buf = <-cw.pool
			cw.offset = 1
		}

		space := cap(cw.buf) - cw.offset
		toWrite := len(p) - written
		if toWrite > space {
			toWrite = space
		}

		copy(cw.buf[cw.offset:cw.offset+toWrite], p[written:written+toWrite])
		cw.offset += toWrite
		written += toWrite

		if cw.offset == cap(cw.buf) {
			cw.buf[0] = protocol.CmdArchiveData
			cw.sendQueue <- cw.buf
			cw.buf = nil
		}
	}
	return written, nil
}

func (cw *tarChunkWriter) flush() {
	if cw.buf != nil && cw.offset > 1 {
		cw.buf[0] = protocol.CmdArchiveData
		cw.sendQueue <- cw.buf[:cw.offset]
		cw.buf = nil
	}
}

// runSendPump pulls chunks from pipeline and sends via socket, waiting for credits.
func (u *TarUploader) runSendPump(ctx context.Context, sendQueue chan []byte) error {
	var firstErr error
	retries := 0

	for buf := range sendQueue {
		if firstErr != nil {
			u.bufPool <- buf[:cap(buf)]
			continue
		}

		select {
		case <-u.Credits:
			if err := u.safeSend(buf); err != nil {
				slog.Error("[TarUploader] Send failed", "err", err)
				firstErr = err
			}

		case <-ctx.Done():
			slog.Debug("[TarUploader] Send pump stopped by context cancellation")
			u.bufPool <- buf[:cap(buf)]
			firstErr = ctx.Err()

		case <-time.After(time.Duration(u.Config.CreditTimeoutSec) * time.Second):
			retries++
			slog.Error("[TarUploader] Credit timeout — server not responding",
				"timeout_sec", u.Config.CreditTimeoutSec,
				"occurrence", retries)
			firstErr = io.ErrUnexpectedEOF
		}

		u.bufPool <- buf[:cap(buf)]
	}

	return firstErr
}

// walkShare recursively walks directory and adds files to TAR.
func (u *TarUploader) walkShare(tw *tar.Writer, key, baseDir string) (int, int64, error) {
	tempDir := ResolveTempDir(u.Config.TempDirs, key)
	var files int
	var totalBytes int64

	err := filepath.WalkDir(baseDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			slog.Debug("[TarUploader] Walk error, skipping entry", "path", path, "err", err)
			return nil
		}
		if d.IsDir() && d.Name() == constants.TempDirName {
			return filepath.SkipDir
		}
		if d.IsDir() && tempDir != "" && path == tempDir {
			return filepath.SkipDir
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(d.Name(), constants.TempExt) {
			return nil
		}

		rel, _ := filepath.Rel(baseDir, path)
		archiveName := key + "/" + filepath.ToSlash(rel)

		tempDir := ResolveTempDir(u.Config.TempDirs, key)
		if err := u.appendFileToTar(context.Background(), tw, path, archiveName, tempDir); err != nil {
			return err
		}

		if info, e := d.Info(); e == nil {
			totalBytes += info.Size()
		}
		files++
		return nil
	})

	return files, totalBytes, err
}

// appendFileToTar creates verified temp copy and packs into TAR.
// Atomicity: source file never locked. MinMaxCredits=3 to avoid deadlock (dual buffer use).
func (u *TarUploader) appendFileToTar(ctx context.Context, tw *tar.Writer, absPath, archiveName, tempDir string) error {
	if u.IsCancelled(archiveName) {
		slog.Debug("[TarUploader] Skipping cancelled file", "archive_name", archiveName)
		return nil
	}

	copyBuf := <-u.copyPool

	result, err := CopyToTemp(ctx, absPath, tempDir, copyBuf, func() bool {
		return u.IsCancelled(archiveName)
	}, u.Config.MaxCopyRetries, u.Config.FileLockRetryDelayMs)

	u.copyPool <- copyBuf

	if err != nil {
		slog.Warn("[TarUploader] Failed to create verified temp copy, skipping",
			"path", absPath, "err", err)
		return nil
	}

	defer os.Remove(result.TempPath)

	file, err := os.Open(result.TempPath)
	if err != nil {
		slog.Error("[TarUploader] Failed to open temp copy", "path", result.TempPath, "err", err)
		return nil
	}
	defer file.Close()

	hdr := &tar.Header{
		Name:     archiveName,
		Size:     result.Size,
		Mode:     0644,
		ModTime:  time.Unix(0, result.MTimeMs*1e6),
		Typeflag: tar.TypeReg,
		Format:   tar.FormatPAX, // PAX required for sub-second mtime precision
	}

	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}

	readBuf := <-u.copyPool
	_, err = io.CopyBuffer(tw, file, readBuf)
	u.copyPool <- readBuf

	if err != nil {
		slog.Error("[TarUploader] Error reading temp copy during TAR append",
			"path", absPath, "err", err)
		return err
	}

	return nil
}

// IsCancelled checks if upload cancelled for path or its parents.
func (u *TarUploader) IsCancelled(archiveName string) bool {
	if u.cancelledPaths == nil {
		return false
	}
	current := archiveName
	for current != "" && current != "." && current != "/" {
		if _, cancelled := u.cancelledPaths.Load(current); cancelled {
			return true
		}
		dir := filepath.ToSlash(filepath.Dir(current))
		if dir == current {
			break
		}
		current = dir
	}
	return false
}
