package watcher

import (
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"Velduct.GoClient/internal/constants"
	"github.com/fsnotify/notify"
)

type FileBatchChangedFunc func(changes map[string][]string)

// Watcher uses github.com/fsnotify/notify for file system event monitoring.
type Watcher struct{
	shares       map[string]string
	tempDirs     map[string]string
	targetShares []string
	onEvent      FileBatchChangedFunc

	debounceDuration time.Duration

	eventCh chan notify.EventInfo
	stopCh  chan struct{}

	mu           sync.Mutex
	globalTimer  *time.Timer
	pendingFiles map[string]struct{}
}

func NewWatcher(shares map[string]string, tempDirs map[string]string, targets []string, eventBufferSize int, debounceDurationMs int, onEvent FileBatchChangedFunc) (*Watcher, error) {
	return &Watcher{
		shares:           shares,
		tempDirs:         tempDirs,
		targetShares:     targets,
		onEvent:          onEvent,
		debounceDuration: time.Duration(debounceDurationMs) * time.Millisecond,
		eventCh:          make(chan notify.EventInfo, eventBufferSize),
		stopCh:           make(chan struct{}),
		pendingFiles:     make(map[string]struct{}),
	}, nil
}

func (w *Watcher) Start() {
	watchedCount := 0
	for key, baseDir := range w.shares {
		if w.isTargetShare(key) {
			continue
		}

		watchPath := baseDir + "/..."
		err := notify.Watch(watchPath, w.eventCh, notify.Write, notify.Create, notify.Rename, notify.Remove)
		if err != nil {
			slog.Error("[Watcher] Failed to add recursive watch", "key", key, "path", baseDir, "err", err)
		} else {
			slog.Info("[Watcher] Recursive watch added", "key", key, "path", baseDir)
			watchedCount++
		}
	}

	slog.Info("[Watcher] Started", "watched_shares", watchedCount, "debounce_ms", w.debounceDuration.Milliseconds())
	go w.listen()
}

func (w *Watcher) Close() {
	notify.Stop(w.eventCh)
	close(w.stopCh)
	slog.Debug("[Watcher] Stopped")
}

func (w *Watcher) listen() {
	for {
		select {
		case <-w.stopCh:
			return
		case ei, ok := <-w.eventCh:
			if !ok {
				slog.Debug("[Watcher] Event channel closed")
				return
			}
			w.handleEvent(ei.Path())
		}
	}
}

func (w *Watcher) handleEvent(path string) {
	if strings.HasSuffix(path, constants.TempExt) || strings.HasSuffix(path, constants.UploadExt) {
		return
	}
	if strings.Contains(path, string(os.PathSeparator)+constants.TempDirName+string(os.PathSeparator)) ||
		strings.HasSuffix(path, string(os.PathSeparator)+constants.TempDirName) {
		return
	}
	for _, tempDir := range w.tempDirs {
		if strings.HasPrefix(path, tempDir+string(os.PathSeparator)) || path == tempDir {
			return
		}
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.pendingFiles[path] = struct{}{}

	if w.globalTimer != nil {
		w.globalTimer.Reset(w.debounceDuration)
	} else {
		w.globalTimer = time.AfterFunc(w.debounceDuration, w.flushFiles)
	}
}

func (w *Watcher) flushFiles() {
	w.mu.Lock()
	filesToProcess := w.pendingFiles
	w.pendingFiles = make(map[string]struct{})
	w.globalTimer = nil
	w.mu.Unlock()

	if len(filesToProcess) == 0 {
		return
	}

	sortedPaths := make([]string, 0, len(filesToProcess))
	for absPath := range filesToProcess {
		sortedPaths = append(sortedPaths, absPath)
	}
	sort.Strings(sortedPaths)

	changes := make(map[string][]string)
	unmatched := 0

	for _, absPath := range sortedPaths {
		matched := false
		for key, baseDir := range w.shares {
			if strings.HasPrefix(absPath, baseDir) {
				changes[key] = append(changes[key], absPath)
				matched = true
				break
			}
		}
		if !matched {
			unmatched++
			slog.Debug("[Watcher] Event path does not match any share, ignoring", "path", absPath)
		}
	}

	if unmatched > 0 {
		slog.Debug("[Watcher] Some events did not match any share", "count", unmatched)
	}

	if len(changes) == 0 {
		return
	}

	total := 0
	for _, paths := range changes {
		total += len(paths)
	}
	slog.Debug("[Watcher] Batch ready for sync", "total_files", total, "shares", len(changes))

	w.onEvent(changes)
}

func (w *Watcher) isTargetShare(key string) bool {
	for _, t := range w.targetShares {
		if t == key {
			return true
		}
	}
	return false
}
