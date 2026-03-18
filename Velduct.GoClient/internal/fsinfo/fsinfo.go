package fsinfo

import (
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// DetectPrecisionMs measures the mtime precision of the filesystem at dir
// by writing probe timestamps and reading them back. Returns precision in ms.
// Falls back to 2000ms (FAT32 worst case) on any error.
func DetectPrecisionMs(dir string) int64 {
	probe := filepath.Join(dir, ".velduct_fs_probe")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return 2000
	}
	if err := os.WriteFile(probe, nil, 0644); err != nil {
		return 2000
	}
	defer os.Remove(probe)

	// Probe 1: odd second — detects FAT32 (2s rounding to even seconds)
	t1 := time.Date(2020, 6, 15, 12, 0, 1, 0, time.UTC)
	if err := os.Chtimes(probe, t1, t1); err != nil {
		return 2000
	}
	info, err := os.Stat(probe)
	if err != nil {
		return 2000
	}
	diff1 := absDiffMs(t1, info.ModTime())
	if diff1 >= 1000 {
		return 2000 // FAT32: 2s precision
	}

	// Probe 2: even second + 1ms — detects HFS+ (1s rounding)
	t2 := time.Date(2020, 6, 15, 12, 0, 0, 1_000_000, time.UTC)
	if err := os.Chtimes(probe, t2, t2); err != nil {
		return 1000
	}
	info, err = os.Stat(probe)
	if err != nil {
		return 1000
	}
	diff2 := absDiffMs(t2, info.ModTime())
	if diff2 >= 1 {
		return 1000 // HFS+: 1s precision
	}

	return 1 // NTFS, APFS, ext4: sub-ms precision
}

// DetectWorstPrecisionMs returns the worst (largest) precision among all paths.
func DetectWorstPrecisionMs(dirs map[string]string) int64 {
	var worst int64 = 1
	for key, dir := range dirs {
		p := DetectPrecisionMs(dir)
		slog.Info("[FSInfo] Detected mtime precision", "share", key, "path", dir, "precisionMs", p)
		if p > worst {
			worst = p
		}
	}
	return worst
}

func absDiffMs(a, b time.Time) int64 {
	d := a.UnixMilli() - b.UnixMilli()
	if d < 0 {
		return -d
	}
	return d
}

// TruncateMs rounds ms down to the nearest multiple of precisionMs.
func TruncateMs(ms, precisionMs int64) int64 {
	if precisionMs <= 1 {
		return ms
	}
	if ms >= 0 {
		return (ms / precisionMs) * precisionMs
	}
	// For negative timestamps (pre-epoch), truncate toward negative infinity
	return ((ms - precisionMs + 1) / precisionMs) * precisionMs
}
