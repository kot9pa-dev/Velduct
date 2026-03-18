package transfer

import (
	"log/slog"
	"sync"
)

// DownloadTracker tracks files being downloaded via temp directory.
// If file is deleted/renamed during download, the transfer is cancelled and temp cleaned up.
// Thread-safe, using sync.Map.
type DownloadTracker struct {
	active    sync.Map // finalPath → tempPath
	cancelled sync.Map // finalPath → struct{} (marked for cancellation)
}

func NewDownloadTracker() *DownloadTracker {
	return &DownloadTracker{}
}

// Register marks the start of file download.
func (dt *DownloadTracker) Register(finalPath, tempPath string) {
	dt.active.Store(finalPath, tempPath)
	slog.Debug("[DownloadTracker] Registered", "final", finalPath, "temp", tempPath)
}

// Unregister removes file from tracker after successful rename or cancellation.
func (dt *DownloadTracker) Unregister(finalPath string) {
	dt.active.Delete(finalPath)
	dt.cancelled.Delete(finalPath)
}

// CancelIfActive marks download for cancellation if active.
// Does not delete temp file—HandleDone/extractEntry does on IsCancelled check.
// Returns true if download was found and marked.
func (dt *DownloadTracker) CancelIfActive(finalPath string) bool {
	if _, ok := dt.active.Load(finalPath); !ok {
		return false
	}
	dt.cancelled.Store(finalPath, struct{}{})
	slog.Debug("[DownloadTracker] Cancelled active download",
		"path", finalPath)
	return true
}

// IsCancelled checks if download was marked for cancellation.
func (dt *DownloadTracker) IsCancelled(finalPath string) bool {
	_, ok := dt.cancelled.Load(finalPath)
	return ok
}
