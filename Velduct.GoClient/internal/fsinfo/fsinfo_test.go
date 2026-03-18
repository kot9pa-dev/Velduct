package fsinfo

import (
	"os"
	"testing"
)

func TestDetectPrecisionMs_ReturnsPositive(t *testing.T) {
	dir := t.TempDir()
	p := DetectPrecisionMs(dir)
	if p <= 0 {
		t.Errorf("Expected positive precision, got %d", p)
	}
	// On most CI/test systems (NTFS, ext4, APFS) precision should be 1ms
	if p != 1 && p != 1000 && p != 2000 {
		t.Errorf("Unexpected precision %dms (expected 1, 1000, or 2000)", p)
	}
}

func TestDetectPrecisionMs_NonexistentDir(t *testing.T) {
	p := DetectPrecisionMs("/nonexistent/path/that/does/not/exist")
	if p != 2000 {
		t.Errorf("Expected 2000 (fallback) for nonexistent dir, got %d", p)
	}
}

func TestDetectWorstPrecisionMs_SingleShare(t *testing.T) {
	dir := t.TempDir()
	p := DetectWorstPrecisionMs(map[string]string{"test": dir})
	if p <= 0 {
		t.Errorf("Expected positive precision, got %d", p)
	}
}

func TestDetectWorstPrecisionMs_MultipleShares(t *testing.T) {
	d1 := t.TempDir()
	d2 := t.TempDir()
	p := DetectWorstPrecisionMs(map[string]string{"a": d1, "b": d2})
	if p <= 0 {
		t.Errorf("Expected positive precision, got %d", p)
	}
}

func TestDetectWorstPrecisionMs_EmptyMap(t *testing.T) {
	p := DetectWorstPrecisionMs(map[string]string{})
	if p != 1 {
		t.Errorf("Expected 1 (default) for empty map, got %d", p)
	}
}

func TestDetectPrecisionMs_CleansUpProbeFile(t *testing.T) {
	dir := t.TempDir()
	DetectPrecisionMs(dir)
	probe := dir + "/.velduct_fs_probe"
	if _, err := os.Stat(probe); !os.IsNotExist(err) {
		t.Error("Probe file should be cleaned up after detection")
	}
}

func TestTruncateMs_Values(t *testing.T) {
	tests := []struct {
		ms, precision, expected int64
	}{
		{1500, 1, 1500},
		{1500, 1000, 1000},
		{2999, 2000, 2000},
		{4000, 2000, 4000},
		{0, 2000, 0},
		{1, 2000, 0},
		{-1, 1, -1},
		{-1500, 1000, -2000},
		{0, 1, 0},
	}
	for _, tt := range tests {
		result := TruncateMs(tt.ms, tt.precision)
		if result != tt.expected {
			t.Errorf("TruncateMs(%d, %d) = %d, want %d", tt.ms, tt.precision, result, tt.expected)
		}
	}
}
