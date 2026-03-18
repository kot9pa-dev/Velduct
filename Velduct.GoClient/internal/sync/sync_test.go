package sync

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Helper to create a test file with a specific mtime.
func createTestFileWithMtime(t *testing.T, path string, mtime time.Time) {
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

// Helper to create a test file with specific size and mtime.
func createTestFileWithSizeAndMtime(t *testing.T, path string, size int, mtime time.Time) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	content := make([]byte, size)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatalf("Failed to set mtime: %v", err)
	}
}

// TestNeedsDownload_FileDoesNotExist tests that needsDownload returns true
// when the file does not exist locally.
func TestNeedsDownload_FileDoesNotExist(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	sh := NewSyncHandler(shares, func([]byte) error { return nil }, nil, 100, 1)

	fileInfo := FileInfo{
		Key:     "share1",
		RelPath: "nonexistent.txt",
		Size:    1000,
		MTimeMs: time.Now().UnixNano() / 1e6,
	}

	result := sh.needsDownload(fileInfo)
	if !result {
		t.Errorf("Expected true (file missing), got %v", result)
	}
}

// --- Precision-based truncation tests (replaces old epsilon tests) ---

func TestNeedsDownload_ExactMatch_Precision1(t *testing.T) {
	tempDir := t.TempDir()
	sh := NewSyncHandler(map[string]string{"s": tempDir}, func([]byte) error { return nil }, nil, 100, 1)
	base := time.Unix(1000, 0)
	createTestFileWithSizeAndMtime(t, filepath.Join(tempDir, "f.txt"), 100, base)

	r := sh.needsDownload(FileInfo{Key: "s", RelPath: "f.txt", Size: 100, MTimeMs: base.UnixNano() / 1e6})
	if r {
		t.Error("Expected false for exact mtime match (precision=1ms)")
	}
}

func TestNeedsDownload_1msDiff_Precision1_ServerNewer(t *testing.T) {
	tempDir := t.TempDir()
	sh := NewSyncHandler(map[string]string{"s": tempDir}, func([]byte) error { return nil }, nil, 100, 1)
	base := time.Unix(1000, 0)
	createTestFileWithSizeAndMtime(t, filepath.Join(tempDir, "f.txt"), 100, base)

	r := sh.needsDownload(FileInfo{Key: "s", RelPath: "f.txt", Size: 100, MTimeMs: base.UnixNano()/1e6 + 1})
	if !r {
		t.Error("Expected true for 1ms diff with precision=1ms (server newer)")
	}
}

func TestNeedsDownload_FAT32_SameBucket(t *testing.T) {
	tempDir := t.TempDir()
	sh := NewSyncHandler(map[string]string{"s": tempDir}, func([]byte) error { return nil }, nil, 100, 2000)
	base := time.Unix(1000, 0)
	createTestFileWithSizeAndMtime(t, filepath.Join(tempDir, "f.txt"), 100, base)

	// 1500ms ahead → both truncate to same 2s bucket → match
	r := sh.needsDownload(FileInfo{Key: "s", RelPath: "f.txt", Size: 100, MTimeMs: base.UnixNano()/1e6 + 1500})
	if r {
		t.Error("Expected false: 1500ms diff within FAT32 2s precision bucket")
	}
}

func TestNeedsDownload_FAT32_DifferentBucket(t *testing.T) {
	tempDir := t.TempDir()
	sh := NewSyncHandler(map[string]string{"s": tempDir}, func([]byte) error { return nil }, nil, 100, 2000)
	base := time.Unix(1000, 0)
	createTestFileWithSizeAndMtime(t, filepath.Join(tempDir, "f.txt"), 100, base)

	// 2000ms ahead → different 2s bucket → pull
	r := sh.needsDownload(FileInfo{Key: "s", RelPath: "f.txt", Size: 100, MTimeMs: base.UnixNano()/1e6 + 2000})
	if !r {
		t.Error("Expected true: server mtime crosses 2s FAT32 bucket boundary")
	}
}

func TestNeedsDownload_HFS_SameBucket(t *testing.T) {
	tempDir := t.TempDir()
	sh := NewSyncHandler(map[string]string{"s": tempDir}, func([]byte) error { return nil }, nil, 100, 1000)
	base := time.Unix(1000, 0)
	createTestFileWithSizeAndMtime(t, filepath.Join(tempDir, "f.txt"), 100, base)

	// 500ms diff → same 1s bucket
	r := sh.needsDownload(FileInfo{Key: "s", RelPath: "f.txt", Size: 100, MTimeMs: base.UnixNano()/1e6 + 500})
	if r {
		t.Error("Expected false: 500ms diff within HFS+ 1s precision bucket")
	}
}

func TestNeedsDownload_HFS_DifferentBucket(t *testing.T) {
	tempDir := t.TempDir()
	sh := NewSyncHandler(map[string]string{"s": tempDir}, func([]byte) error { return nil }, nil, 100, 1000)
	base := time.Unix(1000, 0)
	createTestFileWithSizeAndMtime(t, filepath.Join(tempDir, "f.txt"), 100, base)

	// 1000ms ahead → next 1s bucket
	r := sh.needsDownload(FileInfo{Key: "s", RelPath: "f.txt", Size: 100, MTimeMs: base.UnixNano()/1e6 + 1000})
	if !r {
		t.Error("Expected true: server mtime crosses 1s HFS+ bucket boundary")
	}
}

// TestNeedsDownload_DifferentSize tests that needsDownload returns true
// when file size differs from server, regardless of mtime.
func TestNeedsDownload_DifferentSize(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	sh := NewSyncHandler(shares, func([]byte) error { return nil }, nil, 100, 1)

	baseTime := time.Unix(1000, 0)
	filePath := filepath.Join(tempDir, "test.txt")
	createTestFileWithSizeAndMtime(t, filePath, 1000, baseTime) // Local size 1000

	fileInfo := FileInfo{
		Key:     "share1",
		RelPath: "test.txt",
		Size:    2000, // Server size 2000
		MTimeMs: baseTime.UnixNano() / 1e6,
	}

	result := sh.needsDownload(fileInfo)
	if !result {
		t.Errorf("Expected true (size mismatch), got %v", result)
	}
}

// TestNeedsDownload_ServerNewer tests that needsDownload returns true
// when server's mtime is newer than local and size matches but mtime differs.
func TestNeedsDownload_ServerNewer(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	sh := NewSyncHandler(shares, func([]byte) error { return nil }, nil, 100, 1)

	baseTime := time.Unix(1000, 0)
	filePath := filepath.Join(tempDir, "test.txt")
	createTestFileWithSizeAndMtime(t, filePath, 1000, baseTime)

	// Server mtime is newer (3000ms ahead, outside epsilon)
	serverMtimeMs := baseTime.UnixNano()/1e6 + 3000

	fileInfo := FileInfo{
		Key:     "share1",
		RelPath: "test.txt",
		Size:    1000,
		MTimeMs: serverMtimeMs,
	}

	result := sh.needsDownload(fileInfo)
	if !result {
		t.Errorf("Expected true (server newer), got %v", result)
	}
}

// TestNeedsDownload_ClientNewer tests that needsDownload returns false
// when client's mtime is newer than server and size matches.
func TestNeedsDownload_ClientNewer(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	sh := NewSyncHandler(shares, func([]byte) error { return nil }, nil, 100, 1)

	baseTime := time.Unix(1000, 0)
	filePath := filepath.Join(tempDir, "test.txt")
	createTestFileWithSizeAndMtime(t, filePath, 1000, baseTime)

	// Server mtime is older (3000ms behind, outside epsilon)
	serverMtimeMs := baseTime.UnixNano()/1e6 - 3000

	fileInfo := FileInfo{
		Key:     "share1",
		RelPath: "test.txt",
		Size:    1000,
		MTimeMs: serverMtimeMs,
	}

	result := sh.needsDownload(fileInfo)
	if result {
		t.Errorf("Expected false (client newer), got %v", result)
	}
}

// TestNeedsDownload_UnknownShare tests that needsDownload returns false
// when the share key is unknown.
func TestNeedsDownload_UnknownShare(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	sh := NewSyncHandler(shares, func([]byte) error { return nil }, nil, 100, 1)

	fileInfo := FileInfo{
		Key:     "unknown_share",
		RelPath: "test.txt",
		Size:    1000,
		MTimeMs: time.Now().UnixNano() / 1e6,
	}

	result := sh.needsDownload(fileInfo)
	if result {
		t.Errorf("Expected false (unknown share), got %v", result)
	}
}

// TestNeedsDownload_ServerOlder tests that when server mtime < local, no download.
func TestNeedsDownload_ServerOlder(t *testing.T) {
	tempDir := t.TempDir()
	sh := NewSyncHandler(map[string]string{"s": tempDir}, func([]byte) error { return nil }, nil, 100, 1)

	base := time.Unix(1000, 0)
	createTestFileWithSizeAndMtime(t, filepath.Join(tempDir, "f.txt"), 1000, base)

	r := sh.needsDownload(FileInfo{Key: "s", RelPath: "f.txt", Size: 1000, MTimeMs: base.UnixNano()/1e6 - 1999})
	if r {
		t.Error("Expected false: server older than local → no download")
	}
}

// TestNeedsDownload_ComplexScenarios tests needsDownload with various scenarios.
func TestNeedsDownload_ComplexScenarios(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	sh := NewSyncHandler(shares, func([]byte) error { return nil }, nil, 100, 1)

	baseTime := time.Unix(1000, 0)
	baseMtimeMs := baseTime.UnixNano() / 1e6

	tests := []struct {
		name              string
		filename          string
		localSize         int
		localMtimeMs      int64
		serverSize        int64
		serverMtimeMs     int64
		expectedResult    bool
		description       string
	}{
		{
			name:           "Identical files",
			filename:       "identical.txt",
			localSize:      1000,
			localMtimeMs:   baseMtimeMs,
			serverSize:     1000,
			serverMtimeMs:  baseMtimeMs,
			expectedResult: false,
			description:    "Same size and mtime",
		},
		{
			name:           "Server newer, outside epsilon",
			filename:       "server_newer.txt",
			localSize:      1000,
			localMtimeMs:   baseMtimeMs,
			serverSize:     1000,
			serverMtimeMs:  baseMtimeMs + 3000,
			expectedResult: true,
			description:    "Server 3s newer",
		},
		{
			name:           "Client newer, outside epsilon",
			filename:       "client_newer.txt",
			localSize:      1000,
			localMtimeMs:   baseMtimeMs,
			serverSize:     1000,
			serverMtimeMs:  baseMtimeMs - 3000,
			expectedResult: false,
			description:    "Client 3s newer",
		},
		{
			name:           "Size mismatch, same mtime",
			filename:       "size_mismatch.txt",
			localSize:      1000,
			localMtimeMs:   baseMtimeMs,
			serverSize:     2000,
			serverMtimeMs:  baseMtimeMs,
			expectedResult: true,
			description:    "Different sizes",
		},
		{
			name:           "Very old local file",
			filename:       "old_local.txt",
			localSize:      1000,
			localMtimeMs:   baseMtimeMs - 100000,
			serverSize:     1000,
			serverMtimeMs:  baseMtimeMs,
			expectedResult: true,
			description:    "Client 100s behind",
		},
		{
			name:           "Very recent local modification",
			filename:       "recent_local.txt",
			localSize:      1000,
			localMtimeMs:   baseMtimeMs + 100000,
			serverSize:     1000,
			serverMtimeMs:  baseMtimeMs,
			expectedResult: false,
			description:    "Client 100s ahead",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, tt.filename)
			localTime := time.UnixMilli(tt.localMtimeMs)
			createTestFileWithSizeAndMtime(t, filePath, tt.localSize, localTime)

			fileInfo := FileInfo{
				Key:     "share1",
				RelPath: tt.filename,
				Size:    tt.serverSize,
				MTimeMs: tt.serverMtimeMs,
			}

			result := sh.needsDownload(fileInfo)
			if result != tt.expectedResult {
				t.Errorf("%s: expected %v, got %v", tt.description, tt.expectedResult, result)
			}
		})
	}
}

// TestNeedsDownload_TruncationCoverage tests precision-based truncation comprehensively.
func TestNeedsDownload_TruncationCoverage(t *testing.T) {
	tempDir := t.TempDir()

	baseTime := time.Unix(1000, 0)
	baseMtimeMs := baseTime.UnixNano() / 1e6
	filePath := filepath.Join(tempDir, "trunc_test.txt")
	createTestFileWithSizeAndMtime(t, filePath, 1000, baseTime)

	tests := []struct {
		name        string
		precision   int64
		offsetMs    int64
		expectPull  bool
		description string
	}{
		// Precision=1 (NTFS/ext4): exact comparison
		{"p1 exact", 1, 0, false, "exact match"},
		{"p1 +1ms", 1, 1, true, "server 1ms newer → pull"},
		{"p1 -1ms", 1, -1, false, "server 1ms older → no pull"},

		// Precision=1000 (HFS+): 1s buckets
		{"p1000 +500ms", 1000, 500, false, "same 1s bucket"},
		{"p1000 +999ms", 1000, 999, false, "still same 1s bucket"},
		{"p1000 +1000ms", 1000, 1000, true, "next 1s bucket → pull"},
		{"p1000 -500ms", 1000, -500, false, "same bucket (server older)"},

		// Precision=2000 (FAT32): 2s buckets
		{"p2000 +1999ms", 2000, 1999, false, "same 2s bucket"},
		{"p2000 +2000ms", 2000, 2000, true, "next 2s bucket → pull"},
		{"p2000 -1999ms", 2000, -1999, false, "same bucket (server older)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sh := NewSyncHandler(map[string]string{"s": tempDir}, func([]byte) error { return nil }, nil, 100, tt.precision)

			fi := FileInfo{Key: "s", RelPath: "trunc_test.txt", Size: 1000, MTimeMs: baseMtimeMs + tt.offsetMs}
			result := sh.needsDownload(fi)
			if result != tt.expectPull {
				t.Errorf("%s: expected pull=%v, got %v", tt.description, tt.expectPull, result)
			}
		})
	}
}

// --- truncateMs unit tests ---

func TestTruncateMs(t *testing.T) {
	tests := []struct {
		ms, precision, expected int64
	}{
		{1500, 1, 1500},        // precision=1 → identity
		{1500, 1000, 1000},     // truncate to 1s
		{2999, 2000, 2000},     // truncate to 2s
		{4000, 2000, 4000},     // exact 2s boundary
		{0, 2000, 0},           // zero
		{1, 2000, 0},           // below first bucket
		{-1, 1, -1},            // negative, precision=1
		{-1500, 1000, -2000},   // negative, truncate toward -inf
	}
	for _, tt := range tests {
		result := truncateMs(tt.ms, tt.precision)
		if result != tt.expected {
			t.Errorf("truncateMs(%d, %d) = %d, want %d", tt.ms, tt.precision, result, tt.expected)
		}
	}
}

// TestNeedsDownload_Subdirectories tests that needsDownload works correctly
// with files in subdirectories.
func TestNeedsDownload_Subdirectories(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	sh := NewSyncHandler(shares, func([]byte) error { return nil }, nil, 100, 1)

	baseTime := time.Unix(1000, 0)
	filePath := filepath.Join(tempDir, "deep", "nested", "dir", "file.txt")
	createTestFileWithSizeAndMtime(t, filePath, 1000, baseTime)

	fileInfo := FileInfo{
		Key:     "share1",
		RelPath: "deep/nested/dir/file.txt",
		Size:    1000,
		MTimeMs: baseTime.UnixNano() / 1e6,
	}

	result := sh.needsDownload(fileInfo)
	if result {
		t.Errorf("Expected false (identical file in subdirectory), got %v", result)
	}
}

// TestNeedsDownload_LargeFiles tests that needsDownload works with large files.
func TestNeedsDownload_LargeFiles(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}
	sh := NewSyncHandler(shares, func([]byte) error { return nil }, nil, 100, 1)

	baseTime := time.Unix(1000, 0)
	largeSize := 1024 * 1024 * 100 // 100 MB
	filePath := filepath.Join(tempDir, "large_file.bin")
	createTestFileWithSizeAndMtime(t, filePath, largeSize, baseTime)

	fileInfo := FileInfo{
		Key:     "share1",
		RelPath: "large_file.bin",
		Size:    int64(largeSize),
		MTimeMs: baseTime.UnixNano() / 1e6,
	}

	result := sh.needsDownload(fileInfo)
	if result {
		t.Errorf("Expected false (identical large file), got %v", result)
	}
}

// --- Extra needsDownload variations ---

func TestNeedsDownload_ZeroSizeFile_SameMtime(t *testing.T) {
	tempDir := t.TempDir()
	sh := NewSyncHandler(map[string]string{"s": tempDir}, func([]byte) error { return nil }, nil, 100, 1)

	base := time.Unix(1000, 0)
	createTestFileWithSizeAndMtime(t, filepath.Join(tempDir, "empty.txt"), 0, base)

	r := sh.needsDownload(FileInfo{Key: "s", RelPath: "empty.txt", Size: 0, MTimeMs: base.UnixNano() / 1e6})
	if r {
		t.Error("Expected false for identical zero-size file")
	}
}

func TestNeedsDownload_ZeroSizeLocal_NonZeroServer(t *testing.T) {
	tempDir := t.TempDir()
	sh := NewSyncHandler(map[string]string{"s": tempDir}, func([]byte) error { return nil }, nil, 100, 1)

	base := time.Unix(1000, 0)
	createTestFileWithSizeAndMtime(t, filepath.Join(tempDir, "empty.txt"), 0, base)

	r := sh.needsDownload(FileInfo{Key: "s", RelPath: "empty.txt", Size: 100, MTimeMs: base.UnixNano() / 1e6})
	if !r {
		t.Error("Expected true for size mismatch (0 vs 100)")
	}
}

func TestNeedsDownload_SizeMismatch_ServerOlder(t *testing.T) {
	tempDir := t.TempDir()
	sh := NewSyncHandler(map[string]string{"s": tempDir}, func([]byte) error { return nil }, nil, 100, 1)

	base := time.Unix(1000, 0)
	createTestFileWithSizeAndMtime(t, filepath.Join(tempDir, "f.txt"), 500, base)

	// Server has different size AND older mtime → still needs download (size mismatch)
	r := sh.needsDownload(FileInfo{Key: "s", RelPath: "f.txt", Size: 1000, MTimeMs: base.UnixNano()/1e6 - 5000})
	if !r {
		t.Error("Expected true for size mismatch even when server older")
	}
}

func TestNeedsDownload_MtimeZeroBothSides(t *testing.T) {
	tempDir := t.TempDir()
	sh := NewSyncHandler(map[string]string{"s": tempDir}, func([]byte) error { return nil }, nil, 100, 1)

	epoch := time.Unix(0, 0)
	createTestFileWithSizeAndMtime(t, filepath.Join(tempDir, "f.txt"), 100, epoch)

	r := sh.needsDownload(FileInfo{Key: "s", RelPath: "f.txt", Size: 100, MTimeMs: 0})
	if r {
		t.Error("Expected false for both mtime=0 and same size")
	}
}

func TestHandleServerCheckFiles_MultipleFiles_MixedDecisions(t *testing.T) {
	tempDir := t.TempDir()
	sendCalls := 0
	sh := NewSyncHandler(map[string]string{"s": tempDir}, func([]byte) error {
		sendCalls++
		return nil
	}, nil, 100, 1)

	base := time.Unix(1000, 0)
	baseMs := base.UnixNano() / 1e6

	// File 1: up-to-date (exact match)
	createTestFileWithSizeAndMtime(t, filepath.Join(tempDir, "ok.txt"), 100, base)
	// File 2: server newer (needs download)
	createTestFileWithSizeAndMtime(t, filepath.Join(tempDir, "old.txt"), 100, base)
	// File 3: does not exist (needs download)

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int32(3))

	// File 1: same size, exact mtime → skip
	writeFileEntry(buf, "s", "ok.txt", 100, baseMs)
	// File 2: same size, mtime +5000ms (server newer) → download
	writeFileEntry(buf, "s", "old.txt", 100, baseMs+5000)
	// File 3: missing locally → download
	writeFileEntry(buf, "s", "missing.txt", 200, baseMs)

	result := sh.HandleServerCheckFiles(buf.Bytes())
	if !result {
		t.Error("Expected true (at least one file needs download)")
	}
	if sendCalls == 0 {
		t.Error("Expected sendFunc to be called")
	}
}

func TestHandleServerCheckFiles_DeleteSignal(t *testing.T) {
	tempDir := t.TempDir()
	sh := NewSyncHandler(map[string]string{"s": tempDir}, func([]byte) error { return nil }, nil, 100, 1)

	// Create a file that will be deleted
	createTestFileWithSizeAndMtime(t, filepath.Join(tempDir, "del.txt"), 100, time.Unix(1000, 0))

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int32(1))
	writeFileEntry(buf, "s", "del.txt", -1, 0) // size=-1 → delete

	sh.HandleServerCheckFiles(buf.Bytes())

	if _, err := os.Stat(filepath.Join(tempDir, "del.txt")); !os.IsNotExist(err) {
		t.Error("Expected file to be deleted")
	}
}

func TestHandleServerCheckFiles_EmptyPayload(t *testing.T) {
	sh := NewSyncHandler(map[string]string{"s": t.TempDir()}, func([]byte) error { return nil }, nil, 100, 1)
	if sh.HandleServerCheckFiles([]byte{}) {
		t.Error("Expected false for empty payload")
	}
	if sh.HandleServerCheckFiles(nil) {
		t.Error("Expected false for nil payload")
	}
}

func writeFileEntry(buf *bytes.Buffer, key, path string, size, mtimeMs int64) {
	binary.Write(buf, binary.LittleEndian, int32(len(key)))
	buf.WriteString(key)
	binary.Write(buf, binary.LittleEndian, int32(len(path)))
	buf.WriteString(path)
	binary.Write(buf, binary.LittleEndian, size)
	binary.Write(buf, binary.LittleEndian, mtimeMs)
}

// TestHandleServerCheckFiles_DecodesAndProcessesCorrectly is an integration test
// that verifies the full HandleServerCheckFiles workflow.
func TestHandleServerCheckFiles_DecodesAndProcessesCorrectly(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}

	sendCalls := 0
	sh := NewSyncHandler(shares, func([]byte) error {
		sendCalls++
		return nil
	}, nil, 100, 1)

	baseTime := time.Unix(1000, 0)
	baseMtimeMs := baseTime.UnixNano() / 1e6

	// Create a local file
	filePath := filepath.Join(tempDir, "test.txt")
	createTestFileWithSizeAndMtime(t, filePath, 1000, baseTime)

	// Build a CmdCheckFiles payload with one file
	key := "share1"
	relPath := "test.txt"
	serverSize := int64(1000)
	serverMtimeMs := baseMtimeMs + 3000 // Server is 3s newer

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int32(1)) // 1 file
	binary.Write(buf, binary.LittleEndian, int32(len(key)))
	buf.WriteString(key)
	binary.Write(buf, binary.LittleEndian, int32(len(relPath)))
	buf.WriteString(relPath)
	binary.Write(buf, binary.LittleEndian, serverSize)
	binary.Write(buf, binary.LittleEndian, serverMtimeMs)

	// Handle the payload
	result := sh.HandleServerCheckFiles(buf.Bytes())

	// Should return true because download is needed (server is newer)
	if !result {
		t.Errorf("Expected HandleServerCheckFiles to return true (download requested)")
	}

	// Should have called sendFunc to request the download
	if sendCalls == 0 {
		t.Errorf("Expected sendFunc to be called for pull request")
	}
}

// TestHandleServerCheckFiles_SkipsUpToDateFiles is an integration test
// that verifies files within epsilon are skipped.
func TestHandleServerCheckFiles_SkipsUpToDateFiles(t *testing.T) {
	tempDir := t.TempDir()
	shares := map[string]string{"share1": tempDir}

	sendCalls := 0
	sh := NewSyncHandler(shares, func([]byte) error {
		sendCalls++
		return nil
	}, nil, 100, 1)

	baseTime := time.Unix(1000, 0)
	baseMtimeMs := baseTime.UnixNano() / 1e6

	// Create a local file
	filePath := filepath.Join(tempDir, "test.txt")
	createTestFileWithSizeAndMtime(t, filePath, 1000, baseTime)

	// Build a CmdCheckFiles payload with the file within epsilon
	key := "share1"
	relPath := "test.txt"
	serverSize := int64(1000)
	serverMtimeMs := baseMtimeMs // Exact match — no download needed

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int32(1)) // 1 file
	binary.Write(buf, binary.LittleEndian, int32(len(key)))
	buf.WriteString(key)
	binary.Write(buf, binary.LittleEndian, int32(len(relPath)))
	buf.WriteString(relPath)
	binary.Write(buf, binary.LittleEndian, serverSize)
	binary.Write(buf, binary.LittleEndian, serverMtimeMs)

	// Handle the payload
	result := sh.HandleServerCheckFiles(buf.Bytes())

	// Should return false because no download is needed
	if result {
		t.Errorf("Expected HandleServerCheckFiles to return false (no download needed)")
	}

	// Should not have called sendFunc
	if sendCalls != 0 {
		t.Errorf("Expected sendFunc to not be called for up-to-date file")
	}
}
