package transfer

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"
)

// ResolveTempDir returns temp directory for given share key.
func ResolveTempDir(tempDirs map[string]string, shareKey string) string {
	if dir, ok := tempDirs[shareKey]; ok {
		return dir
	}
	slog.Warn("[TempDir] No temp dir configured for share, this should not happen", "key", shareKey)
	return ""
}

// EnsureTempDirs creates all configured temp directories.
func EnsureTempDirs(tempDirs map[string]string) {
	for key, dir := range tempDirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			slog.Error("[TempDir] Failed to create temp dir", "key", key, "path", dir, "err", err)
		} else {
			slog.Debug("[TempDir] Ensured temp dir exists", "key", key, "path", dir)
		}
	}
}

// CopyFileResult contains result of atomic file copy to temp directory.
type CopyFileResult struct {
	TempPath string
	Size     int64
	MTimeMs  int64
}

// CopyToTemp copies file to temp directory with metadata verification.
// Retries if file changes during copy. maxRetries limits retry attempts.
func CopyToTemp(
	ctx context.Context,
	absPath string,
	tempDir string,
	buf []byte,
	cancelCheck func() bool,
	maxRetries int,
	retryDelayMs int,
) (CopyFileResult, error) {
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return CopyFileResult{}, fmt.Errorf("create temp dir: %w", err)
	}

	retryDelay := time.Duration(retryDelayMs) * time.Millisecond
	metaRetries := 0

	for {
		select {
		case <-ctx.Done():
			return CopyFileResult{}, ctx.Err()
		default:
		}

		if cancelCheck != nil && cancelCheck() {
			return CopyFileResult{}, fmt.Errorf("cancelled by caller")
		}

		infoBefore, err := os.Stat(absPath)
		if err != nil {
			if os.IsNotExist(err) {
				return CopyFileResult{}, err
			}
			slog.Debug("[TempCopy] Stat failed (locked?), retrying", "path", absPath, "err", err)
			time.Sleep(retryDelay)
			continue
		}

		if infoBefore.IsDir() {
			return CopyFileResult{}, fmt.Errorf("path is a directory: %s", absPath)
		}

		if infoBefore.Size() == 0 {
			dst, err := os.CreateTemp(tempDir, "ul-*.tmp")
			if err != nil {
				return CopyFileResult{}, fmt.Errorf("create temp file: %w", err)
			}
			dst.Close()
			return CopyFileResult{
				TempPath: dst.Name(),
				Size:     0,
				MTimeMs:  infoBefore.ModTime().UnixNano() / 1e6,
			}, nil
		}

		src, err := os.Open(absPath)
		if err != nil {
			if os.IsNotExist(err) {
				return CopyFileResult{}, err
			}
			slog.Debug("[TempCopy] Open failed (locked?), retrying", "path", absPath, "err", err)
			time.Sleep(retryDelay)
			continue
		}

		dst, err := os.CreateTemp(tempDir, "ul-*.tmp")
		if err != nil {
			src.Close()
			return CopyFileResult{}, fmt.Errorf("create temp file: %w", err)
		}

		_, copyErr := io.CopyBuffer(dst, src, buf)
		src.Close()
		dst.Close()

		if copyErr != nil {
			os.Remove(dst.Name())
			if os.IsNotExist(copyErr) {
				return CopyFileResult{}, copyErr
			}
			slog.Debug("[TempCopy] Copy failed, retrying", "path", absPath, "err", copyErr)
			time.Sleep(retryDelay)
			continue
		}

		infoAfter, err := os.Stat(absPath)
		if err != nil {
			os.Remove(dst.Name())
			return CopyFileResult{}, err
		}

		if infoBefore.Size() == infoAfter.Size() &&
			infoBefore.ModTime().Equal(infoAfter.ModTime()) {
			return CopyFileResult{
				TempPath: dst.Name(),
				Size:     infoBefore.Size(),
				MTimeMs:  infoBefore.ModTime().UnixNano() / 1e6,
			}, nil
		}

		os.Remove(dst.Name())
		metaRetries++

		if metaRetries >= maxRetries {
			slog.Warn("[TempCopy] File keeps changing during copy, giving up",
				"path", absPath,
				"retries", metaRetries,
				"size_before", infoBefore.Size(),
				"size_after", infoAfter.Size())
			return CopyFileResult{}, fmt.Errorf(
				"file changed during copy after %d retries: %s", metaRetries, absPath)
		}

		slog.Debug("[TempCopy] File changed during copy, retrying",
			"path", absPath,
			"attempt", metaRetries,
			"size_before", infoBefore.Size(),
			"size_after", infoAfter.Size(),
			"mtime_before", infoBefore.ModTime(),
			"mtime_after", infoAfter.ModTime())

		time.Sleep(retryDelay)
	}
}
