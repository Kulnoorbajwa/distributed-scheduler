package models

import "time"

// JobStatus represents every possible state a job can be in
type JobStatus string

const (
	JobStatusPending    JobStatus = "PENDING"
	JobStatusDispatched JobStatus = "DISPATCHED"
	JobStatusRunning    JobStatus = "RUNNING"
	JobStatusSucceeded  JobStatus = "SUCCEEDED"
	JobStatusFailed     JobStatus = "FAILED"
	JobStatusCancelled  JobStatus = "CANCELLED"
	JobStatusTimedOut   JobStatus = "TIMED_OUT"
	JobStatusDeadLetter JobStatus = "DEAD_LETTER"
)

// JobPriority controls queue ordering
type JobPriority string

const (
	JobPriorityHigh   JobPriority = "HIGH"
	JobPriorityMedium JobPriority = "MEDIUM"
	JobPriorityLow    JobPriority = "LOW"
)

// Job is the core unit of work in the system
type Job struct {
	ID             string // unique job ID
	RequestID      string // idempotency key from client
	TenantID       string // which tenant submitted this
	Status         JobStatus
	Priority       JobPriority
	Payload        string // the actual work to do (JSON)
	MaxRetries     int
	RetryCount     int
	LastError      string
	AssignedWorker string    // worker ID currently holding this job
	LeaseID        string    // current active lease ID
	LeaseExpiresAt time.Time // when the lease expires
	LeaseVersion   int       // increments every reassignment
	CreatedAt      time.Time
	UpdatedAt      time.Time
	DispatchedAt   *time.Time
	StartedAt      *time.Time
	CompletedAt    *time.Time
	RunTimeout     time.Duration
	RetryAfter     *time.Time
}

// WorkerStatus represents whether a worker is alive
type WorkerStatus string

const (
	WorkerStatusActive   WorkerStatus = "ACTIVE"
	WorkerStatusDead     WorkerStatus = "DEAD"
	WorkerStatusDraining WorkerStatus = "DRAINING" // graceful shutdown
)

// Worker represents a node that executes jobs
type Worker struct {
	ID            string
	TenantID      string
	Status        WorkerStatus
	Address       string // host:port for gRPC
	LastHeartbeat time.Time
	RegisteredAt  time.Time
	ActiveJobs    int    // currently running jobs
	MaxJobs       int    // capacity
	CPUSlots      int    // available CPU slots
	MemoryMB      int    // available memory
	Version       string // binary version for rolling upgrades
}

// Lease tracks job ownership by a worker
type Lease struct {
	ID        string
	JobID     string
	WorkerID  string
	Version   int // stale lease check — must match job's LeaseVersion
	ExpiresAt time.Time
	CreatedAt time.Time
	RenewedAt time.Time
}

// MissedRunPolicy controls what happens when a scheduled run is missed
type MissedRunPolicy string

const (
	MissedRunPolicySkip    MissedRunPolicy = "SKIP"
	MissedRunPolicyRunOnce MissedRunPolicy = "RUN_ONCE"
	MissedRunPolicyRunAll  MissedRunPolicy = "RUN_ALL"
)

// Schedule represents a recurring job definition
type Schedule struct {
	ID           string
	TenantID     string
	Name         string
	CronExpr     string
	Payload      string
	Priority     JobPriority
	MaxRetries   int
	RunTimeout   time.Duration
	Enabled      bool
	MissedPolicy MissedRunPolicy
	LastRunAt    *time.Time
	NextRunAt    time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// JobTransition is an audit log entry for every state change
type JobTransition struct {
	ID        string
	JobID     string
	FromState JobStatus
	ToState   JobStatus
	WorkerID  string
	Reason    string
	CreatedAt time.Time
}
