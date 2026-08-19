package config

type TestPull struct {
	Key  string `json:"key"`
	Path string `json:"path"`
}

type Config struct {
	ServerURL    string            `json:"server_url"`
	Shares       map[string]string `json:"shares"`
	TargetShares []string          `json:"target_shares"`
	TestPulls    []TestPull        `json:"test_pulls"`

	// TempDirs maps share key to per-share temp directory for atomic operations.
	// Must be on same filesystem as share for atomic os.Rename.
	TempDirs map[string]string `json:"temp_dirs"`

	ReconnectIntervalSec   int `json:"reconnect_interval_sec"`
	MaxCredits             int `json:"max_credits"`
	ChunkSizeBytes         int `json:"chunk_size_bytes"`
	ReadBufferSize         int `json:"read_buffer_size"`
	WriteBufferOverhead    int `json:"write_buffer_overhead"`
	CreditTimeoutSec       int `json:"credit_timeout_sec"`
	SingleUploadTimeoutSec int `json:"single_upload_timeout_sec"`

	LivenessIdleTimeoutSec  int `json:"liveness_idle_timeout_sec"`
	LivenessPingIntervalSec int `json:"liveness_ping_interval_sec"`
	HandshakeTimeoutSec     int `json:"handshake_timeout_sec"`

	ScanBatchSize          int `json:"scan_batch_size"`
	UploadQueueSize        int `json:"upload_queue_size"`
	DebounceDurationMs     int `json:"debounce_duration_ms"`
	StartupSyncDelayMs     int `json:"startup_sync_delay_ms"`
	BatchCoalescingMaxMs   int `json:"batch_coalescing_max_ms"`
	BatchCoalescingIdleMs  int `json:"batch_coalescing_idle_ms"`

	MaxCopyRetries       int `json:"max_copy_retries"`
	FileLockRetryDelayMs int `json:"file_lock_retry_delay_ms"`

	WatcherEventBufferSize int `json:"watcher_event_buffer_size"`

	PprofAddress string `json:"pprof_address"`

	JwtKey        string `json:"jwt_key"`
	JwtIssuer     string `json:"jwt_issuer"`
	JwtAudience   string `json:"jwt_audience"`
	JwtTTLSeconds int    `json:"jwt_ttl_seconds"`

	// Detected at startup; not from config file
	FsPrecisionMs int64 `json:"-"`
}
