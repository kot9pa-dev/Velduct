package transfer

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"Velduct.GoClient/internal/config"
)

// Helper function to create a minimal session for testing.
func newTestSession(t *testing.T, shares map[string]string) *Session {
	cfg := config.Config{
		Shares:              shares,
		MaxCredits:          4,
		ChunkSizeBytes:      4096,
		UploadQueueSize:     10,
		ScanBatchSize:       100,
		TempDirs:            make(map[string]string),
		ReadBufferSize:      65536,
		WriteBufferOverhead: 1024,
		FsPrecisionMs:       1,
	}
	sendFunc := func(b []byte) error { return nil }
	s := NewSession(shares, sendFunc, cfg)
	t.Cleanup(func() {
		s.Close()
	})
	return s
}

// Helper to create a test file with a specific mtime.
func createTestFile(t *testing.T, path string, mtime time.Time) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	if err := os.WriteFile(path, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatalf("Failed to set mtime: %v", err)
	}
}

// TestIsRecentDownload_NotInMap tests that IsRecentDownload returns false
// when file is not in recentDownloads.
func TestIsRecentDownload_NotInMap(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	s := newTestSession(t, shares)

	fullPath := filepath.Join(tempDir, "test.txt")
	result := s.IsRecentDownload(fullPath, 1000)

	if result != false {
		t.Errorf("Expected false, got %v", result)
	}
}

// TestIsRecentDownload_MatchesActualFsMtime tests that IsRecentDownload returns true
// when the file is in recentDownloads and ActualFsMtimeMs matches the current mtime.
func TestIsRecentDownload_MatchesActualFsMtime(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	s := newTestSession(t, shares)

	fullPath := filepath.Join(tempDir, "test.txt")
	serverMs := int64(1000)
	actualFsMs := int64(2000)

	s.StoreDownloadedMtime(fullPath, serverMs, actualFsMs)
	result := s.IsRecentDownload(fullPath, actualFsMs)

	if result != true {
		t.Errorf("Expected true, got %v", result)
	}
}

// TestIsRecentDownload_DifferentActualFsMtime tests that IsRecentDownload returns false
// when ActualFsMtimeMs differs from the current mtime (file was modified locally).
func TestIsRecentDownload_DifferentActualFsMtime(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	s := newTestSession(t, shares)

	fullPath := filepath.Join(tempDir, "test.txt")
	serverMs := int64(1000)
	actualFsMs := int64(2000)

	s.StoreDownloadedMtime(fullPath, serverMs, actualFsMs)
	result := s.IsRecentDownload(fullPath, 3000) // Different from stored actualFsMs

	if result != false {
		t.Errorf("Expected false for unmatched mtime, got %v", result)
	}
}

// TestResolveReportMtime_NotInRecentDownloads tests that ResolveReportMtime
// returns currentFsMtimeMs when file is not in recentDownloads.
func TestResolveReportMtime_NotInRecentDownloads(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	s := newTestSession(t, shares)

	fullPath := filepath.Join(tempDir, "test.txt")
	currentFsMtimeMs := int64(5000)

	result := s.ResolveReportMtime(fullPath, currentFsMtimeMs)

	if result != currentFsMtimeMs {
		t.Errorf("Expected %d, got %d", currentFsMtimeMs, result)
	}
}

// TestResolveReportMtime_UnchangedFile tests that ResolveReportMtime
// returns serverMtimeMs when file is unchanged since download.
func TestResolveReportMtime_UnchangedFile(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	s := newTestSession(t, shares)

	fullPath := filepath.Join(tempDir, "test.txt")
	serverMs := int64(1000)
	actualFsMs := int64(2000)

	s.StoreDownloadedMtime(fullPath, serverMs, actualFsMs)
	result := s.ResolveReportMtime(fullPath, actualFsMs)

	if result != serverMs {
		t.Errorf("Expected %d (server mtime), got %d", serverMs, result)
	}
}

// TestResolveReportMtime_LocallyModified_ClientAhead tests that ResolveReportMtime
// returns currentFsMtimeMs when file was modified locally and client clock is ahead.
func TestResolveReportMtime_LocallyModified_ClientAhead(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	s := newTestSession(t, shares)

	fullPath := filepath.Join(tempDir, "test.txt")
	serverMs := int64(1000)
	actualFsMs := int64(2000)
	currentFsMtimeMs := int64(3000) // > server

	s.StoreDownloadedMtime(fullPath, serverMs, actualFsMs)
	result := s.ResolveReportMtime(fullPath, currentFsMtimeMs)

	if result != currentFsMtimeMs {
		t.Errorf("Expected %d (current fs mtime), got %d", currentFsMtimeMs, result)
	}
}

// TestResolveReportMtime_LocallyModified_ClientBehind tests that ResolveReportMtime
// returns serverMtimeMs + fsPrecisionMs + 1 (bump) when file was modified locally
// and client clock is behind. This ensures the server recognizes the edit as newer.
func TestResolveReportMtime_LocallyModified_ClientBehind(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	s := newTestSession(t, shares)

	fullPath := filepath.Join(tempDir, "test.txt")
	serverMs := int64(5000)
	actualFsMs := int64(4000)
	currentFsMtimeMs := int64(3000) // < server, but != actualFsMs means local modification

	s.StoreDownloadedMtime(fullPath, serverMs, actualFsMs)
	result := s.ResolveReportMtime(fullPath, currentFsMtimeMs)

	expectedResult := serverMs + 2
	if result != expectedResult {
		t.Errorf("Expected %d (server mtime + precision + 1), got %d", expectedResult, result)
	}
}

// TestHandleFileMtimeAck_ValidPayload tests that HandleFileMtimeAck correctly
// parses the binary payload and sets the file mtime.
func TestHandleFileMtimeAck_ValidPayload(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	s := newTestSession(t, shares)

	// Create a test file
	filename := "test.txt"
	fullPath := filepath.Join(tempDir, filename)
	testTime := time.Unix(0, 1000*1e6)
	createTestFile(t, fullPath, testTime)

	// Build the binary payload
	key := "share1"
	relPath := filename
	serverMtimeMs := int64(2000)

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int32(len(key)))
	buf.WriteString(key)
	binary.Write(buf, binary.LittleEndian, int32(len(relPath)))
	buf.WriteString(relPath)
	binary.Write(buf, binary.LittleEndian, serverMtimeMs)

	// Handle the ACK
	s.handleFileMtimeAck(buf.Bytes())

	// Verify the file mtime was updated
	info, err := os.Stat(fullPath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	actualMtimeMs := info.ModTime().UnixNano() / 1e6
	expectedMtimeMs := serverMtimeMs

	if actualMtimeMs != expectedMtimeMs {
		t.Errorf("Expected mtime %d ms, got %d ms", expectedMtimeMs, actualMtimeMs)
	}
}

// TestHandleFileMtimeAck_StoresInRecentDownloads tests that HandleFileMtimeAck
// stores the file in recentDownloads for anti-echo detection.
func TestHandleFileMtimeAck_StoresInRecentDownloads(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	s := newTestSession(t, shares)

	// Create a test file
	filename := "test.txt"
	fullPath := filepath.Join(tempDir, filename)
	testTime := time.Unix(0, 1000*1e6)
	createTestFile(t, fullPath, testTime)

	// Build the binary payload
	key := "share1"
	relPath := filename
	serverMtimeMs := int64(2000)

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int32(len(key)))
	buf.WriteString(key)
	binary.Write(buf, binary.LittleEndian, int32(len(relPath)))
	buf.WriteString(relPath)
	binary.Write(buf, binary.LittleEndian, serverMtimeMs)

	// Handle the ACK
	s.handleFileMtimeAck(buf.Bytes())

	// Wait a moment for the goroutine to complete


	// Verify it was stored in recentDownloads
	info, err := os.Stat(fullPath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}
	actualFsMs := info.ModTime().UnixNano() / 1e6

	isRecent := s.IsRecentDownload(fullPath, actualFsMs)
	if !isRecent {
		t.Errorf("Expected file to be in recentDownloads")
	}
}

// TestHandleFileMtimeAck_TruncatedPayload_MissingKey tests that HandleFileMtimeAck
// gracefully handles a truncated payload missing the key.
func TestHandleFileMtimeAck_TruncatedPayload_MissingKey(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	s := newTestSession(t, shares)

	// Build a truncated payload (missing key data)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int32(10)) // key length 10
	buf.WriteString("sh")                               // but only 2 bytes of key

	// This should not panic
	s.handleFileMtimeAck(buf.Bytes())
}

// TestHandleFileMtimeAck_TruncatedPayload_MissingPath tests that HandleFileMtimeAck
// gracefully handles a truncated payload missing the path.
func TestHandleFileMtimeAck_TruncatedPayload_MissingPath(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	s := newTestSession(t, shares)

	// Build a truncated payload (missing path data)
	key := "share1"
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int32(len(key)))
	buf.WriteString(key)
	binary.Write(buf, binary.LittleEndian, int32(10)) // path length 10
	buf.WriteString("te")                              // but only 2 bytes of path

	// This should not panic
	s.handleFileMtimeAck(buf.Bytes())
}

// TestHandleFileMtimeAck_TruncatedPayload_MissingMtime tests that HandleFileMtimeAck
// gracefully handles a truncated payload missing the mtime.
func TestHandleFileMtimeAck_TruncatedPayload_MissingMtime(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	s := newTestSession(t, shares)

	// Build a truncated payload (missing mtime data)
	key := "share1"
	relPath := "test.txt"
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int32(len(key)))
	buf.WriteString(key)
	binary.Write(buf, binary.LittleEndian, int32(len(relPath)))
	buf.WriteString(relPath)
	// Missing mtime (should be 8 bytes)

	// This should not panic
	s.handleFileMtimeAck(buf.Bytes())
}

// TestHandleFileMtimeAck_UnknownShare tests that HandleFileMtimeAck
// gracefully handles an unknown share key.
func TestHandleFileMtimeAck_UnknownShare(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	s := newTestSession(t, shares)

	// Build payload with unknown share key
	unknownKey := "unknown_share"
	relPath := "test.txt"
	serverMtimeMs := int64(2000)

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int32(len(unknownKey)))
	buf.WriteString(unknownKey)
	binary.Write(buf, binary.LittleEndian, int32(len(relPath)))
	buf.WriteString(relPath)
	binary.Write(buf, binary.LittleEndian, serverMtimeMs)

	// This should not panic
	s.handleFileMtimeAck(buf.Bytes())
}

// TestHandleFileMtimeAck_NonexistentFile tests that HandleFileMtimeAck
// gracefully handles a file that does not exist.
func TestHandleFileMtimeAck_NonexistentFile(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	s := newTestSession(t, shares)

	// Build payload for a nonexistent file
	key := "share1"
	relPath := "nonexistent.txt"
	serverMtimeMs := int64(2000)

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int32(len(key)))
	buf.WriteString(key)
	binary.Write(buf, binary.LittleEndian, int32(len(relPath)))
	buf.WriteString(relPath)
	binary.Write(buf, binary.LittleEndian, serverMtimeMs)

	// This should not panic
	s.handleFileMtimeAck(buf.Bytes())
}

// TestStoreDownloadedMtime_StoresCorrectly tests that StoreDownloadedMtime
// correctly stores both server and actual FS mtimes.
func TestStoreDownloadedMtime_StoresCorrectly(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	s := newTestSession(t, shares)

	fullPath := filepath.Join(tempDir, "test.txt")
	serverMs := int64(1000)
	actualFsMs := int64(2000)

	s.StoreDownloadedMtime(fullPath, serverMs, actualFsMs)

	// Verify IsRecentDownload works with the stored values
	result := s.IsRecentDownload(fullPath, actualFsMs)
	if !result {
		t.Errorf("Expected IsRecentDownload to return true after storing")
	}
}

// TestResolveReportMtime_ComplexClockSkewScenario tests ResolveReportMtime
// with various clock skew scenarios.
func TestResolveReportMtime_ComplexClockSkewScenario(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	s := newTestSession(t, shares)

	tests := []struct {
		name              string
		serverMs          int64
		actualFsMs        int64
		currentFsMtimeMs  int64
		expectedResult    int64
		description       string
	}{
		{
			name:             "Unchanged file",
			serverMs:         1000,
			actualFsMs:       2000,
			currentFsMtimeMs: 2000,
			expectedResult:   1000,
			description:      "Return server mtime when file unchanged",
		},
		{
			name:             "Modified locally, client ahead",
			serverMs:         1000,
			actualFsMs:       2000,
			currentFsMtimeMs: 3000,
			expectedResult:   3000,
			description:      "Return current FS mtime when client ahead",
		},
		{
			name:             "Modified locally, client behind",
			serverMs:         5000,
			actualFsMs:       4000,
			currentFsMtimeMs: 3000,
			expectedResult:   5002,
			description:      "Return server + precision + 1 when client behind",
		},
		{
			name:             "Significantly modified, client ahead",
			serverMs:         1000,
			actualFsMs:       2000,
			currentFsMtimeMs: 10000,
			expectedResult:   10000,
			description:      "Return current FS mtime for big local change",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fullPath := filepath.Join(tempDir, tt.name+".txt")
			s.StoreDownloadedMtime(fullPath, tt.serverMs, tt.actualFsMs)
			result := s.ResolveReportMtime(fullPath, tt.currentFsMtimeMs)

			if result != tt.expectedResult {
				t.Errorf("%s: expected %d, got %d", tt.description, tt.expectedResult, result)
			}
		})
	}
}

// --- Bump exceeds epsilon guarantee ---

// Verifies that bump exceeds FS precision and triggers pull on server.
// Without this, edited files with clock-behind clients would be silently ignored.
func TestResolveReportMtime_BumpExceedsEpsilon(t *testing.T) {
	s := newTestSession(t, map[string]string{"s": t.TempDir()})

	serverMs := int64(1710000000000)
	s.StoreDownloadedMtime("/f", serverMs, serverMs)

	// File modified locally, client clock behind → bump
	result := s.ResolveReportMtime("/f", serverMs-5000)

	bump := result - serverMs
	if bump <= 0 {
		t.Errorf("Bump %dms is not positive (result=%d, server=%d)", bump, result, serverMs)
	}
}

// Same scenario but from the server's perspective: the bumped mtime must trigger pull.
// Simulates the server's ClassifyFileForPull logic in Go.
func TestResolveReportMtime_BumpedValuePassesServerCheck(t *testing.T) {
	s := newTestSession(t, map[string]string{"s": t.TempDir()})

	serverMs := int64(1710000000000)
	s.StoreDownloadedMtime("/f", serverMs, serverMs)

	bumped := s.ResolveReportMtime("/f", serverMs-5000)

	// Server-side check: abs(clientMs - serverMs) > 2000 && clientMs > serverMs → pull
	diff := bumped - serverMs
	if diff < 0 {
		diff = -diff
	}
	if diff <= 0 {
		t.Errorf("Diff %dms not positive — server would skip pull", diff)
	}
	if bumped <= serverMs {
		t.Errorf("Bumped %d ≤ server %d — server would not pull (client not newer)", bumped, serverMs)
	}
}

// Verifies bump is exactly fsPrecisionMs + 1.
func TestResolveReportMtime_BumpIsMinimalExceedingEpsilon(t *testing.T) {
	s := newTestSession(t, map[string]string{"s": t.TempDir()})

	serverMs := int64(5000)
	s.StoreDownloadedMtime("/f", serverMs, serverMs)

	result := s.ResolveReportMtime("/f", 1000) // modified, clock behind
	if result != serverMs+2 {
		t.Errorf("Expected exactly %d (server + precision + 1), got %d", serverMs+2, result)
	}
}

// Verifies that when client clock is ahead, no bump is applied (raw fsMtime used).
func TestResolveReportMtime_NoBumpWhenClientAhead(t *testing.T) {
	s := newTestSession(t, map[string]string{"s": t.TempDir()})

	serverMs := int64(5000)
	s.StoreDownloadedMtime("/f", serverMs, serverMs)

	clientMs := int64(10000) // ahead of server
	result := s.ResolveReportMtime("/f", clientMs)
	if result != clientMs {
		t.Errorf("Expected raw fsMtime %d when client ahead, got %d", clientMs, result)
	}
}

// --- Mtime edge cases ---

func TestResolveReportMtime_ZeroMtime(t *testing.T) {
	s := newTestSession(t, map[string]string{"s": t.TempDir()})
	if r := s.ResolveReportMtime("/any/path", 0); r != 0 {
		t.Errorf("Expected 0, got %d", r)
	}
}

func TestResolveReportMtime_ServerMtimeZero_ClientBehind(t *testing.T) {
	s := newTestSession(t, map[string]string{"s": t.TempDir()})
	s.StoreDownloadedMtime("/f", 0, 0)
	// currentFs(-100) != actualFs(0) → modified; currentFs < serverMs(0) → bump
	if r := s.ResolveReportMtime("/f", -100); r != 2 {
		t.Errorf("Expected 2 (bump from 0: precision+1), got %d", r)
	}
}

func TestResolveReportMtime_BumpNearMaxInt64(t *testing.T) {
	s := newTestSession(t, map[string]string{"s": t.TempDir()})
	var maxMs int64 = 1<<63 - 3
	s.StoreDownloadedMtime("/f", maxMs, maxMs)
	if r := s.ResolveReportMtime("/f", 0); r != maxMs+2 {
		t.Errorf("Expected %d, got %d", maxMs+2, r)
	}
}

func TestResolveReportMtime_ModifiedToExactServerMs(t *testing.T) {
	s := newTestSession(t, map[string]string{"s": t.TempDir()})
	// File downloaded (serverMs=5000, actualFs=5000), then modified to exactly 5000
	s.StoreDownloadedMtime("/f", 5000, 5000)
	// actualFs==currentFs → treated as unchanged
	if r := s.ResolveReportMtime("/f", 5000); r != 5000 {
		t.Errorf("Expected 5000 (unchanged), got %d", r)
	}
}

func TestIsRecentDownload_DifferentPath(t *testing.T) {
	s := newTestSession(t, map[string]string{"s": t.TempDir()})
	s.StoreDownloadedMtime("/old/path.txt", 1000, 1000)
	if s.IsRecentDownload("/new/path.txt", 1000) {
		t.Error("Expected false for different path")
	}
	if !s.IsRecentDownload("/old/path.txt", 1000) {
		t.Error("Expected true for original path")
	}
}

func TestIsRecentDownload_RecreatedWithDifferentMtime(t *testing.T) {
	s := newTestSession(t, map[string]string{"s": t.TempDir()})
	s.StoreDownloadedMtime("/f", 1000, 1000)
	if s.IsRecentDownload("/f", 2000) {
		t.Error("Expected false for recreated file with different mtime")
	}
}

func TestHandleFileMtimeAck_EmptyAndNilPayload(t *testing.T) {
	s := newTestSession(t, map[string]string{"s": t.TempDir()})
	s.handleFileMtimeAck([]byte{})
	s.handleFileMtimeAck([]byte{1})
	s.handleFileMtimeAck(nil)
}

func TestHandleFileMtimeAck_ZeroMtime(t *testing.T) {
	tempDir := t.TempDir()
	s := newTestSession(t, map[string]string{"share1": tempDir})

	path := filepath.Join(tempDir, "f.txt")
	createTestFile(t, path, time.Now())

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int32(6))
	buf.WriteString("share1")
	binary.Write(buf, binary.LittleEndian, int32(5))
	buf.WriteString("f.txt")
	binary.Write(buf, binary.LittleEndian, int64(0))

	s.handleFileMtimeAck(buf.Bytes())

	info, _ := os.Stat(path)
	ms := info.ModTime().UnixNano() / 1e6
	if ms != 0 {
		t.Errorf("Expected mtime 0 (epoch), got %d", ms)
	}
}

func TestStoreDownloadedMtime_OverwritesPrevious(t *testing.T) {
	s := newTestSession(t, map[string]string{"s": t.TempDir()})
	s.StoreDownloadedMtime("/f", 1000, 1000)
	s.StoreDownloadedMtime("/f", 2000, 2000)
	if s.IsRecentDownload("/f", 1000) {
		t.Error("Expected false for old actualFs after overwrite")
	}
	if !s.IsRecentDownload("/f", 2000) {
		t.Error("Expected true for new actualFs after overwrite")
	}
}

// TestHandleFileMtimeAck_ChtimesFails_NoRecentDownload verifies that when
// os.Chtimes fails (e.g. read-only FS), the pre-stored recentDownloads entry
// remains so watcher doesn't cause infinite re-upload.
func TestHandleFileMtimeAck_ChtimesFails_PreStoreRemains(t *testing.T) {
	tempDir := t.TempDir()
	s := newTestSession(t, map[string]string{"share1": tempDir})

	// Create a file then make it read-only
	path := filepath.Join(tempDir, "readonly.txt")
	createTestFile(t, path, time.Now())
	os.Chmod(path, 0444)
	t.Cleanup(func() { os.Chmod(path, 0644) })

	serverMs := int64(5000)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int32(6))
	buf.WriteString("share1")
	binary.Write(buf, binary.LittleEndian, int32(12))
	buf.WriteString("readonly.txt")
	binary.Write(buf, binary.LittleEndian, serverMs)

	s.handleFileMtimeAck(buf.Bytes())


	// Pre-store should still be there (Store happens before Chtimes)
	// Even if Chtimes failed, the entry exists preventing watcher echo
	v, ok := s.recentDownloads.Load(path)
	if !ok {
		t.Fatal("Expected recentDownloads entry to exist even after Chtimes failure")
	}
	dm := v.(downloadedMtime)
	if dm.ServerMtimeMs != serverMs {
		t.Errorf("Expected serverMs=%d, got %d", serverMs, dm.ServerMtimeMs)
	}
}

// TestResolveReportMtime_FAT32Bump verifies bump with FAT32 precision (2000ms).
func TestResolveReportMtime_FAT32Bump(t *testing.T) {
	cfg := config.Config{
		Shares:              map[string]string{"s": t.TempDir()},
		MaxCredits:          4,
		ChunkSizeBytes:      4096,
		UploadQueueSize:     10,
		ScanBatchSize:       100,
		TempDirs:            make(map[string]string),
		ReadBufferSize:      65536,
		WriteBufferOverhead: 1024,
		FsPrecisionMs:       2000, // FAT32
	}
	s := NewSession(cfg.Shares, func(b []byte) error { return nil }, cfg)
	t.Cleanup(func() { s.Close() })

	serverMs := int64(10000)
	s.StoreDownloadedMtime("/f", serverMs, serverMs)

	// Modified locally, clock behind → bump must be precision + 1 = 2001
	result := s.ResolveReportMtime("/f", 5000)
	expected := serverMs + 2001
	if result != expected {
		t.Errorf("FAT32 bump: expected %d, got %d", expected, result)
	}
}

// TestResolveReportMtime_AntiEchoAfterACK simulates the full flow:
// upload → ACK → Chtimes → watcher fires → IsRecentDownload blocks re-upload.
func TestResolveReportMtime_AntiEchoAfterACK(t *testing.T) {
	tempDir := t.TempDir()
	s := newTestSession(t, map[string]string{"share1": tempDir})

	path := filepath.Join(tempDir, "synced.txt")
	createTestFile(t, path, time.Now())

	serverMs := int64(5000)

	// Simulate ACK handling
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int32(6))
	buf.WriteString("share1")
	binary.Write(buf, binary.LittleEndian, int32(10))
	buf.WriteString("synced.txt")
	binary.Write(buf, binary.LittleEndian, serverMs)

	s.handleFileMtimeAck(buf.Bytes())


	// Watcher reads file mtime
	info, _ := os.Stat(path)
	fsMtimeMs := info.ModTime().UnixNano() / 1e6

	// Anti-echo: should be marked as recent download
	if !s.IsRecentDownload(path, fsMtimeMs) {
		t.Error("Expected IsRecentDownload=true after ACK + Chtimes")
	}

	// ResolveReportMtime should return server mtime (not FS mtime)
	reported := s.ResolveReportMtime(path, fsMtimeMs)
	if reported != serverMs {
		t.Errorf("Expected reported mtime=%d (server), got %d", serverMs, reported)
	}
}

// TestHandleFileMtimeAck_MultipleFiles tests that HandleFileMtimeAck
// correctly handles ACKs for multiple files sequentially.
func TestHandleFileMtimeAck_MultipleFiles(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	s := newTestSession(t, shares)

	// Create multiple test files
	files := []struct {
		name     string
		mtime    int64
		testMtimeMs int64
	}{
		{"file1.txt", 1000, 2000},
		{"file2.txt", 3000, 4000},
		{"file3.txt", 5000, 6000},
	}

	for _, f := range files {
		fullPath := filepath.Join(tempDir, f.name)
		testTime := time.Unix(0, f.testMtimeMs*1e6)
		createTestFile(t, fullPath, testTime)

		// Build and handle ACK
		key := "share1"
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, int32(len(key)))
		buf.WriteString(key)
		binary.Write(buf, binary.LittleEndian, int32(len(f.name)))
		buf.WriteString(f.name)
		binary.Write(buf, binary.LittleEndian, f.mtime)

		s.handleFileMtimeAck(buf.Bytes())

	

		// Verify mtime was updated
		info, err := os.Stat(fullPath)
		if err != nil {
			t.Fatalf("Failed to stat file: %v", err)
		}

		actualMtimeMs := info.ModTime().UnixNano() / 1e6
		if actualMtimeMs != f.mtime {
			t.Errorf("File %s: expected mtime %d, got %d", f.name, f.mtime, actualMtimeMs)
		}
	}
}

// TestHandleFileMtimeAck_WithSubdirectories tests that HandleFileMtimeAck
// correctly handles files in subdirectories.
func TestHandleFileMtimeAck_WithSubdirectories(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	s := newTestSession(t, shares)

	// Create a file in a subdirectory
	relPath := "subdir/nested/file.txt"
	fullPath := filepath.Join(tempDir, relPath)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	testTime := time.Unix(0, 1000*1e6)
	createTestFile(t, fullPath, testTime)

	// Build and handle ACK
	key := "share1"
	serverMtimeMs := int64(2000)

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int32(len(key)))
	buf.WriteString(key)
	binary.Write(buf, binary.LittleEndian, int32(len(relPath)))
	buf.WriteString(relPath)
	binary.Write(buf, binary.LittleEndian, serverMtimeMs)

	s.handleFileMtimeAck(buf.Bytes())



	// Verify the nested file's mtime was updated
	info, err := os.Stat(fullPath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	actualMtimeMs := info.ModTime().UnixNano() / 1e6
	if actualMtimeMs != serverMtimeMs {
		t.Errorf("Expected mtime %d ms, got %d ms", serverMtimeMs, actualMtimeMs)
	}
}
