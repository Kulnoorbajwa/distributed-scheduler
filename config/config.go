package config

import "time"

type Config struct {
	// Server
	SchedulerPort int
	WorkerPort    int

	// Heartbeat
	HeartbeatInterval time.Duration // how often workers ping
	HeartbeatTimeout  time.Duration // how long before marked dead
	MissedHeartbeats  int           // how many misses before dead

	// Lease
	LeaseDuration time.Duration // how long a job lease lasts
	LeaseRenewAt  time.Duration // renew when this much time left
	GracePeriod   time.Duration // extra time before reaping

	// Retries
	MaxRetries     int
	RetryBaseDelay time.Duration // exponential backoff base
	RetryMaxDelay  time.Duration // cap on backoff

	// Timeouts
	DispatchTimeout time.Duration // time to assign a job
	RunTimeout      time.Duration // max time a job can run

	// Queue
	MaxQueueLength  int
	MaxPayloadBytes int
	MaxWorkerCount  int

	// Database
	DatabaseURL string

	// Observability
	MetricsPort int
	LogLevel    string // "debug", "info", "warn", "error"

	// Security
	APIToken          string // shared secret for client → scheduler auth
	WorkerTokenSecret string // shared secret for worker → scheduler auth
	TLSEnabled        bool
	TLSCertFile       string
	TLSKeyFile        string

	// Rate limiting
	RateLimitPerSecond int           // max requests per second per tenant
	RateLimitBurst     int           // burst allowance above steady rate
	RateLimitWindow    time.Duration // sliding window for cleanup

	// Tenant
	DefaultTenantQuota int // max jobs per tenant in queue
}

func Default() *Config {
	return &Config{
		// Server
		SchedulerPort: 50051,
		WorkerPort:    50052,

		// Heartbeat
		HeartbeatInterval: 5 * time.Second,
		HeartbeatTimeout:  15 * time.Second,
		MissedHeartbeats:  3,

		// Lease
		LeaseDuration: 30 * time.Second,
		LeaseRenewAt:  10 * time.Second,
		GracePeriod:   5 * time.Second,

		// Retries
		MaxRetries:     5,
		RetryBaseDelay: 2 * time.Second,
		RetryMaxDelay:  60 * time.Second,

		// Timeouts
		DispatchTimeout: 10 * time.Second,
		RunTimeout:      5 * time.Minute,

		// Queue
		MaxQueueLength:  10000,
		MaxPayloadBytes: 1048576, // 1MB
		MaxWorkerCount:  100,

		// Database
		DatabaseURL: "", // must be set via DATABASE_URL env var

		// Observability
		MetricsPort: 9090,
		LogLevel:    "info",

		// Security — no defaults; must be set via env vars
		APIToken:          "",
		WorkerTokenSecret: "",
		TLSEnabled:        false,

		// Rate limiting
		RateLimitPerSecond: 50,
		RateLimitBurst:     100,
		RateLimitWindow:    1 * time.Minute,

		// Tenant
		DefaultTenantQuota: 1000,
	}
}
