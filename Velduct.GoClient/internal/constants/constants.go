package constants

const (
	UploadExt   = ".upload"
	TempExt     = ".tmp"
	TempDirName = ".ddzs_temp"

	DefaultConfigPath = "config.json"

	EnvConfigPath = "CONFIG_PATH"

	EnvServerURL              = "SERVER_URL"
	EnvReconnectInterval      = "RECONNECT_INTERVAL"
	EnvMaxCredits             = "MAX_CREDITS"
	EnvChunkSizeBytes         = "CHUNK_SIZE_BYTES"
	EnvReadBufferSize         = "READ_BUFFER_SIZE"
	EnvWriteBufferOverhead    = "WRITE_BUFFER_OVERHEAD"
	EnvCreditTimeoutSec       = "CREDIT_TIMEOUT_SEC"
	EnvSingleUploadTimeoutSec = "SINGLE_UPLOAD_TIMEOUT_SEC"

	EnvLivenessIdleTimeoutSec  = "LIVENESS_IDLE_TIMEOUT_SEC"
	EnvLivenessPingIntervalSec = "LIVENESS_PING_INTERVAL_SEC"
	EnvHandshakeTimeoutSec     = "HANDSHAKE_TIMEOUT_SEC"

	EnvScanBatchSize          = "SCAN_BATCH_SIZE"
	EnvUploadQueueSize        = "UPLOAD_QUEUE_SIZE"
	EnvDebounceDurationMs     = "DEBOUNCE_DURATION_MS"
	EnvStartupSyncDelayMs     = "STARTUP_SYNC_DELAY_MS"
	EnvBatchCoalescingMaxMs   = "BATCH_COALESCING_MAX_MS"
	EnvBatchCoalescingIdleMs  = "BATCH_COALESCING_IDLE_MS"

	EnvMaxCopyRetries       = "MAX_COPY_RETRIES"
	EnvFileLockRetryDelayMs = "FILE_LOCK_RETRY_DELAY_MS"

	EnvWatcherEventBufferSize = "WATCHER_EVENT_BUFFER_SIZE"

	EnvPprofAddress = "PPROF_ADDRESS"

	EnvJwtKey        = "JWT_KEY"
	EnvJwtIssuer     = "JWT_ISSUER"
	EnvJwtAudience   = "JWT_AUDIENCE"
	EnvJwtTTLSeconds = "JWT_TTL_SECONDS"
)
