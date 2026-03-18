package transfer

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"Velduct.GoClient/internal/constants"
)

// CleanupTempFiles cleans up on startup: removes orphaned temp files and legacy .tmp files.
// Called before server connection to avoid interfering with active transfers.
func CleanupTempFiles(tempDirs map[string]string, shares map[string]string) {
	totalTemp := 0
	totalLegacy := 0

	for key, tempDir := range tempDirs {
		removedTemp := cleanTempDir(tempDir)
		totalTemp += removedTemp

		if removedTemp > 0 {
			slog.Debug("[Cleanup] Cleaned temp directory",
				"key", key, "removed", removedTemp, "dir", tempDir)
		}
	}

	for key, baseDir := range shares {
		removedLegacy := cleanLegacyTmpFiles(baseDir, tempDirs[key])
		totalLegacy += removedLegacy

		if removedLegacy > 0 {
			slog.Debug("[Cleanup] Removed legacy .tmp files",
				"key", key, "count", removedLegacy)
		}
	}

	if totalTemp > 0 || totalLegacy > 0 {
		slog.Debug("[Cleanup] Total cleanup complete",
			"temp_files", totalTemp, "legacy_tmp", totalLegacy)
	}
}

// cleanTempDir removes all files from temp directory.
func cleanTempDir(tempDir string) int {
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		return 0
	}

	removed := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(tempDir, entry.Name())
		if err := os.Remove(path); err != nil {
			slog.Debug("[Cleanup] Failed to remove temp file", "path", path, "err", err)
		} else {
			removed++
			slog.Debug("[Cleanup] Removed orphaned temp file", "path", path)
		}
	}
	return removed
}

// cleanLegacyTmpFiles removes .tmp files from share tree for backward compatibility.
func cleanLegacyTmpFiles(baseDir string, tempDir string) int {
	removed := 0

	filepath.WalkDir(baseDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			slog.Debug("[Cleanup] WalkDir error, skipping", "path", path, "err", err)
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
			if err := os.Remove(path); err != nil {
				slog.Debug("[Cleanup] Failed to remove legacy temp file", "path", path, "err", err)
			} else {
				removed++
				slog.Debug("[Cleanup] Removed legacy temp file", "path", path)
			}
		}
		return nil
	})

	return removed
}
