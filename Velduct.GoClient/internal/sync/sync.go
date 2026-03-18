package sync

import (
	"bytes"
	"encoding/binary"
	"log/slog"
	"os"
	"path/filepath"

	"Velduct.GoClient/internal/protocol"
)

// FileInfo describes file metadata received from server.
type FileInfo struct {
	Key     string
	RelPath string
	Size    int64
	MTimeMs int64
}

// SyncHandler handles incoming CmdCheckFiles and determines which files to download or delete.
type SyncHandler struct {
	shares            map[string]string
	sendFunc          func([]byte) error
	cancelDownload    func(string) bool
	isRecentDownload  func(string, int64) bool
	scanBatchSize     int
}

func NewSyncHandler(shares map[string]string, sendFunc func([]byte) error, cancelDownload func(string) bool, isRecentDownload func(string, int64) bool, scanBatchSize int) *SyncHandler {
	return &SyncHandler{
		shares:           shares,
		sendFunc:         sendFunc,
		cancelDownload:   cancelDownload,
		isRecentDownload: isRecentDownload,
		scanBatchSize:    scanBatchSize,
	}
}

// HandleServerCheckFiles handles incoming CmdCheckFiles from server.
// For size > 0: downloads if local is outdated. For size == -1: deletes locally.
// Returns true if download requests were sent.
func (sh *SyncHandler) HandleServerCheckFiles(payload []byte) bool {
	files := decodeCheckFilesPayload(payload)
	if len(files) == 0 {
		return false
	}

	var filesToPull []FileInfo
	var filesToDelete []FileInfo

	for _, f := range files {
		if f.Size == -1 {
			filesToDelete = append(filesToDelete, f)
			continue
		}

		if f.Size == -2 {
			sh.handleDirectoryCreate(f)
			continue
		}

		if sh.needsDownload(f) {
			filesToPull = append(filesToPull, f)
		}
	}

	for _, f := range filesToDelete {
		sh.deleteLocal(f)
	}

	if len(filesToPull) > 0 {
		slog.Debug("[SyncHandler] Requesting download from server", "files", len(filesToPull))
		for i := 0; i < len(filesToPull); i += sh.scanBatchSize {
			end := i + sh.scanBatchSize
			if end > len(filesToPull) {
				end = len(filesToPull)
			}
			sh.sendBatchPull(filesToPull[i:end])
		}
		return true
	}

	return false
}

// needsDownload checks if file should be downloaded from server.
func (sh *SyncHandler) needsDownload(f FileInfo) bool {
	baseDir, ok := sh.shares[f.Key]
	if !ok {
		slog.Debug("[SyncHandler] Unknown share key, skipping", "key", f.Key)
		return false
	}

	fullPath := filepath.Join(baseDir, filepath.FromSlash(f.RelPath))
	info, err := os.Stat(fullPath)
	if err != nil {
		// Файл не существует локально — нужно скачать
		return true
	}

	localSize := info.Size()
	localMtimeMs := info.ModTime().UnixNano() / 1e6

	if localSize == f.Size && localMtimeMs == f.MTimeMs {
		return false
	}

	// Anti-echo: if file was recently downloaded and mtime matches saved value,
	// difference is only due to FS rounding (FAT32→2s, NTFS→100ns).
	if localSize == f.Size && sh.isRecentDownload != nil && sh.isRecentDownload(fullPath, localMtimeMs) {
		slog.Debug("[SyncHandler] Skipping re-download (mtime rounded by FS)",
			"key", f.Key, "path", f.RelPath,
			"serverMs", f.MTimeMs, "localMs", localMtimeMs)
		return false
	}

	return f.MTimeMs > localMtimeMs
}

// deleteLocal deletes file or directory locally.
func (sh *SyncHandler) deleteLocal(f FileInfo) {
	baseDir, ok := sh.shares[f.Key]
	if !ok {
		return
	}

	fullPath := filepath.Join(baseDir, filepath.FromSlash(f.RelPath))

	if sh.cancelDownload != nil {
		if sh.cancelDownload(fullPath) {
			slog.Debug("[SyncHandler] Cancelled active download before delete",
				"key", f.Key, "path", f.RelPath)
		}
	}

	info, err := os.Stat(fullPath)
	if err != nil {
		return
	}

	if info.IsDir() {
		if err := os.RemoveAll(fullPath); err != nil {
			slog.Warn("[SyncHandler] Failed to delete directory", "path", fullPath, "err", err)
		} else {
			slog.Debug("[SyncHandler] Deleted directory", "key", f.Key, "path", f.RelPath)
		}
	} else {
		if err := os.Remove(fullPath); err != nil {
			slog.Warn("[SyncHandler] Failed to delete file", "path", fullPath, "err", err)
		} else {
			slog.Debug("[SyncHandler] Deleted file", "key", f.Key, "path", f.RelPath)
		}
	}
}

// handleDirectoryCreate creates directory locally.
func (sh *SyncHandler) handleDirectoryCreate(f FileInfo) {
	baseDir, ok := sh.shares[f.Key]
	if !ok {
		return
	}

	fullPath := filepath.Join(baseDir, filepath.FromSlash(f.RelPath))
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		slog.Warn("[SyncHandler] Failed to create directory", "path", fullPath, "err", err)
	}
}

// sendBatchPull sends CmdBatchPull to request file downloads.
func (sh *SyncHandler) sendBatchPull(files []FileInfo) {
	buf := new(bytes.Buffer)
	buf.WriteByte(protocol.CmdBatchPull)
	binary.Write(buf, binary.LittleEndian, int32(len(files)))

	for _, f := range files {
		binary.Write(buf, binary.LittleEndian, int32(len(f.Key)))
		buf.WriteString(f.Key)
		binary.Write(buf, binary.LittleEndian, int32(len(f.RelPath)))
		buf.WriteString(f.RelPath)
		binary.Write(buf, binary.LittleEndian, f.Size)
		binary.Write(buf, binary.LittleEndian, f.MTimeMs)
	}

	if err := sh.sendFunc(buf.Bytes()); err != nil {
		slog.Error("[SyncHandler] Failed to send CmdBatchPull", "files", len(files), "err", err)
	}
}

// decodeCheckFilesPayload decodes CmdCheckFiles payload. Opcode already removed.
func decodeCheckFilesPayload(payload []byte) []FileInfo {
	if len(payload) < 4 {
		return nil
	}

	pos := 0
	count := int(int32(payload[pos]) | int32(payload[pos+1])<<8 | int32(payload[pos+2])<<16 | int32(payload[pos+3])<<24)
	pos += 4

	if count <= 0 {
		return nil
	}

	files := make([]FileInfo, 0, count)

	for i := 0; i < count; i++ {
		if pos+4 > len(payload) {
			break
		}
		kLen := int(int32(payload[pos]) | int32(payload[pos+1])<<8 | int32(payload[pos+2])<<16 | int32(payload[pos+3])<<24)
		pos += 4

		if pos+kLen > len(payload) {
			break
		}
		key := string(payload[pos : pos+kLen])
		pos += kLen

		if pos+4 > len(payload) {
			break
		}
		pLen := int(int32(payload[pos]) | int32(payload[pos+1])<<8 | int32(payload[pos+2])<<16 | int32(payload[pos+3])<<24)
		pos += 4

		if pos+pLen > len(payload) {
			break
		}
		relPath := string(payload[pos : pos+pLen])
		pos += pLen

		if pos+16 > len(payload) {
			break
		}
		size := int64(payload[pos]) | int64(payload[pos+1])<<8 | int64(payload[pos+2])<<16 | int64(payload[pos+3])<<24 |
			int64(payload[pos+4])<<32 | int64(payload[pos+5])<<40 | int64(payload[pos+6])<<48 | int64(payload[pos+7])<<56
		pos += 8
		mtimeMs := int64(payload[pos]) | int64(payload[pos+1])<<8 | int64(payload[pos+2])<<16 | int64(payload[pos+3])<<24 |
			int64(payload[pos+4])<<32 | int64(payload[pos+5])<<40 | int64(payload[pos+6])<<48 | int64(payload[pos+7])<<56
		pos += 8

		files = append(files, FileInfo{
			Key:     key,
			RelPath: relPath,
			Size:    size,
			MTimeMs: mtimeMs,
		})
	}

	return files
}

// RegisterShares sends CMD_REGISTER_SHARES to server.
func RegisterShares(sendFunc func([]byte) error, shares map[string]string, targetShares []string) {
	allKeys := make(map[string]struct{})
	for key := range shares {
		allKeys[key] = struct{}{}
	}
	for _, key := range targetShares {
		allKeys[key] = struct{}{}
	}

	buf := new(bytes.Buffer)
	buf.WriteByte(protocol.CmdRegisterShares)
	binary.Write(buf, binary.LittleEndian, int32(len(allKeys)))

	for key := range allKeys {
		binary.Write(buf, binary.LittleEndian, int32(len(key)))
		buf.WriteString(key)
	}

	if err := sendFunc(buf.Bytes()); err != nil {
		slog.Error("[Sync] Failed to send CMD_REGISTER_SHARES", "err", err)
	} else {
		slog.Info("[Sync] Registered shares with server", "count", len(allKeys))
	}

}
