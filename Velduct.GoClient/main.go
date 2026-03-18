package main

import (
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"net/http"
	_ "net/http/pprof"

	"Velduct.GoClient/internal/client"
	"Velduct.GoClient/internal/config"
	"Velduct.GoClient/internal/constants"
	"Velduct.GoClient/internal/transfer"
	"Velduct.GoClient/internal/watcher"
)

func envStr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func envInt(name string, fallback int) int {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		slog.Warn("Invalid env var value, using config.json value",
			"env", name, "value", v, "fallback", fallback)
		return fallback
	}
	return n
}

// Resolves config file path in priority order: flag -config, env CONFIG_PATH, default config.json.
func resolveConfigPath() string {
	configFlag := flag.Lookup("config")
	if configFlag != nil && configFlag.Value.String() != "" {
		return configFlag.Value.String()
	}
	if v := os.Getenv(constants.EnvConfigPath); v != "" {
		return v
	}
	return constants.DefaultConfigPath
}

func loadConfig() config.Config {
	cfg := config.Config{
		ServerURL:              "ws://127.0.0.1:5050/ws",
		Shares:                 make(map[string]string),
		ReconnectIntervalSec:   5,
		MaxCredits:             12,
		ChunkSizeBytes:         4194304,
		ReadBufferSize:         65536,
		WriteBufferOverhead:    4096,
		CreditTimeoutSec:       60,
		SingleUploadTimeoutSec: 30,
		ScanBatchSize:          100,
		UploadQueueSize:        500,
		DebounceDurationMs:     2500,
		StartupSyncDelayMs:     500,
		MaxCopyRetries:         10,
		FileLockRetryDelayMs:   500,
		WatcherEventBufferSize: 512,
		PprofAddress:           "localhost:6060",
		JwtIssuer:              "ddzs-client",
		JwtAudience:            "VelductServer",
		JwtTTLSeconds:          300,
	}

	configPath := resolveConfigPath()
	file, err := os.Open(configPath)
	if err == nil {
		defer file.Close()
		slog.Info("Loading config", "path", configPath)
		if err := json.NewDecoder(file).Decode(&cfg); err != nil {
			slog.Error("Failed to parse config", "path", configPath, "err", err)
			os.Exit(1)
		}
	} else {
		slog.Warn("Config file not found, using defaults + env vars", "path", configPath)
	}

	cfg.ServerURL              = envStr(constants.EnvServerURL, cfg.ServerURL)
	cfg.ReconnectIntervalSec   = envInt(constants.EnvReconnectInterval, cfg.ReconnectIntervalSec)
	cfg.MaxCredits             = envInt(constants.EnvMaxCredits, cfg.MaxCredits)
	cfg.ChunkSizeBytes         = envInt(constants.EnvChunkSizeBytes, cfg.ChunkSizeBytes)
	cfg.ReadBufferSize         = envInt(constants.EnvReadBufferSize, cfg.ReadBufferSize)
	cfg.WriteBufferOverhead    = envInt(constants.EnvWriteBufferOverhead, cfg.WriteBufferOverhead)
	cfg.CreditTimeoutSec       = envInt(constants.EnvCreditTimeoutSec, cfg.CreditTimeoutSec)
	cfg.SingleUploadTimeoutSec = envInt(constants.EnvSingleUploadTimeoutSec, cfg.SingleUploadTimeoutSec)
	cfg.ScanBatchSize          = envInt(constants.EnvScanBatchSize, cfg.ScanBatchSize)
	cfg.UploadQueueSize        = envInt(constants.EnvUploadQueueSize, cfg.UploadQueueSize)
	cfg.DebounceDurationMs     = envInt(constants.EnvDebounceDurationMs, cfg.DebounceDurationMs)
	cfg.StartupSyncDelayMs     = envInt(constants.EnvStartupSyncDelayMs, cfg.StartupSyncDelayMs)
	cfg.MaxCopyRetries         = envInt(constants.EnvMaxCopyRetries, cfg.MaxCopyRetries)
	cfg.FileLockRetryDelayMs   = envInt(constants.EnvFileLockRetryDelayMs, cfg.FileLockRetryDelayMs)
	cfg.WatcherEventBufferSize = envInt(constants.EnvWatcherEventBufferSize, cfg.WatcherEventBufferSize)
	cfg.PprofAddress           = envStr(constants.EnvPprofAddress, cfg.PprofAddress)
	cfg.JwtKey                 = envStr(constants.EnvJwtKey, cfg.JwtKey)
	cfg.JwtIssuer              = envStr(constants.EnvJwtIssuer, cfg.JwtIssuer)
	cfg.JwtAudience            = envStr(constants.EnvJwtAudience, cfg.JwtAudience)
	cfg.JwtTTLSeconds          = envInt(constants.EnvJwtTTLSeconds, cfg.JwtTTLSeconds)

	return cfg
}

func validateConfig(cfg config.Config) {
	if cfg.JwtKey == "" {
		slog.Error("FATAL: JWT key is not set",
			"hint", "set jwt_key in config.json or JWT_KEY env var (min 32 chars)")
		os.Exit(1)
	}
	if len(cfg.JwtKey) < 32 {
		slog.Warn("JWT key is shorter than 32 characters — consider using a stronger key",
			"length", len(cfg.JwtKey))
	}
	if cfg.MaxCredits < 3 {
		slog.Error("FATAL: max_credits must be >= 3 to avoid deadlock in TAR upload",
			"current", cfg.MaxCredits)
		os.Exit(1)
	}
}

func validateTempDirs(cfg config.Config) {
	sharePaths := make(map[string]string)
	for key, dir := range cfg.Shares {
		abs, err := filepath.Abs(dir)
		if err != nil {
			abs = dir
		}
		sharePaths[filepath.Clean(abs)] = key
	}

	for tempKey, tempDir := range cfg.TempDirs {
		abs, err := filepath.Abs(tempDir)
		if err != nil {
			abs = tempDir
		}
		cleanTemp := filepath.Clean(abs)

		if shareKey, conflict := sharePaths[cleanTemp]; conflict {
			slog.Error("FATAL: temp directory path conflicts with share directory",
				"temp_key", tempKey,
				"temp_path", tempDir,
				"share_key", shareKey,
				"share_path", cfg.Shares[shareKey])
			os.Exit(1)
		}
	}
}

func main() {
	flag.String("config", "", "path to config file (default: config.json, also can use CONFIG_PATH env var)")
	flag.Parse()

	cfg := loadConfig()
	validateConfig(cfg)

	if cfg.PprofAddress != "" {
		go func() {
			slog.Info("Starting pprof", "address", cfg.PprofAddress)
			if err := http.ListenAndServe(cfg.PprofAddress, nil); err != nil {
				slog.Error("pprof server failed", "err", err)
			}
		}()
	}

	slog.Info("Starting Client",
		"server", cfg.ServerURL,
		"max_credits", cfg.MaxCredits,
		"chunk_size", cfg.ChunkSizeBytes,
		"scan_batch", cfg.ScanBatchSize,
		"upload_queue", cfg.UploadQueueSize,
		"debounce_ms", cfg.DebounceDurationMs)

	for k, p := range cfg.Shares {
		slog.Info("Configured share", "key", k, "path", p)
	}

	if cfg.TempDirs == nil {
		cfg.TempDirs = make(map[string]string)
	}
	for key, shareDir := range cfg.Shares {
		if _, ok := cfg.TempDirs[key]; !ok {
			cfg.TempDirs[key] = filepath.Join(shareDir, constants.TempDirName)
		}
	}

	validateTempDirs(cfg)

	for k, p := range cfg.TempDirs {
		slog.Info("Configured temp dir", "key", k, "path", p)
	}

	transfer.EnsureTempDirs(cfg.TempDirs)
	transfer.CleanupTempFiles(cfg.TempDirs, cfg.Shares)

	var currentClient *client.Client

	fileWatcher, err := watcher.NewWatcher(cfg.Shares, cfg.TempDirs, cfg.TargetShares, cfg.WatcherEventBufferSize, cfg.DebounceDurationMs, func(changes map[string][]string) {
		if currentClient != nil {
			currentClient.SyncMultipleFiles(changes)
		}
	})

	if err != nil {
		slog.Error("Failed to start watcher", "error", err)
	} else {
		fileWatcher.Start()
		slog.Info("File Watcher started")
		defer fileWatcher.Close()
	}

	for {
		c := client.NewClient(cfg)
		currentClient = c

		err := c.Connect()
		if err != nil {
			slog.Error("Connection failed", "error", err, "retry_in", cfg.ReconnectIntervalSec)
			currentClient = nil
			time.Sleep(time.Duration(cfg.ReconnectIntervalSec) * time.Second)
			continue
		}

		go func() {
			time.Sleep(time.Duration(cfg.StartupSyncDelayMs) * time.Millisecond)
			c.SyncAllShares()
		}()

		c.Run()

		currentClient = nil
		c.Close()

		slog.Warn("Disconnected", "retry_in", cfg.ReconnectIntervalSec)
		time.Sleep(time.Duration(cfg.ReconnectIntervalSec) * time.Second)
	}
}
