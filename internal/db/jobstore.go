package db

import (
	"context"
	"fmt"
	"time"

	"github.com/Kulnoorbajwa/distributed-scheduler/internal/models"
	"github.com/jackc/pgx/v5"
)

// JobStore handles all job-related database operations
type JobStore struct {
	db *DB
}

// NewJobStore creates a new JobStore
func NewJobStore(db *DB) *JobStore {
	return &JobStore{db: db}
}

// CreateJob inserts a new job — idempotent via request_id
func (s *JobStore) CreateJob(ctx context.Context, job *models.Job) (*models.Job, error) {
	query := `
		INSERT INTO jobs (
			request_id, tenant_id, status, priority,
			payload, max_retries, run_timeout_ms
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7
		)
		ON CONFLICT (request_id) DO NOTHING
		RETURNING id, created_at, updated_at`

	row := s.db.Pool.QueryRow(ctx, query,
		job.RequestID,
		job.TenantID,
		models.JobStatusPending,
		job.Priority,
		job.Payload,
		job.MaxRetries,
		job.RunTimeout.Milliseconds(),
	)

	err := row.Scan(&job.ID, &job.CreatedAt, &job.UpdatedAt)
	if err == pgx.ErrNoRows {
		// request_id already exists — idempotent, not an error
		return s.GetJobByRequestID(ctx, job.RequestID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create job: %w", err)
	}

	// Record the state transition in audit log
	if err := s.recordTransition(ctx, job.ID, "", models.JobStatusPending, "", "job created"); err != nil {
		return nil, err
	}

	return job, nil
}

// GetJobByID fetches a single job by its ID
func (s *JobStore) GetJobByID(ctx context.Context, jobID string) (*models.Job, error) {
	query := `
		SELECT
			id, request_id, tenant_id, status, priority,
			payload, max_retries, retry_count, last_error,
			assigned_worker, lease_id, lease_expires_at,
			lease_version, run_timeout_ms,
			created_at, updated_at, dispatched_at,
			started_at, completed_at
		FROM jobs WHERE id = $1`

	row := s.db.Pool.QueryRow(ctx, query, jobID)
	return scanJob(row)
}

// GetJobByRequestID fetches a job by its idempotency key
func (s *JobStore) GetJobByRequestID(ctx context.Context, requestID string) (*models.Job, error) {
	query := `
		SELECT
			id, request_id, tenant_id, status, priority,
			payload, max_retries, retry_count, last_error,
			assigned_worker, lease_id, lease_expires_at,
			lease_version, run_timeout_ms,
			created_at, updated_at, dispatched_at,
			started_at, completed_at
		FROM jobs WHERE request_id = $1`

	row := s.db.Pool.QueryRow(ctx, query, requestID)
	return scanJob(row)
}

// ClaimNextJob atomically claims the next available job for a worker
// This is the most critical query in the system — must be atomic
func (s *JobStore) ClaimNextJob(ctx context.Context, workerID string, leaseDuration time.Duration) (*models.Job, error) {
	query := `
		UPDATE jobs SET
			status          = 'DISPATCHED',
			assigned_worker = $1,
			lease_id        = uuid_generate_v4(),
			lease_expires_at = NOW() + $2::interval,
			lease_version   = lease_version + 1,
			dispatched_at   = NOW()
		WHERE id = (
			SELECT id FROM jobs
			WHERE status = 'PENDING'
			ORDER BY
				CASE priority
					WHEN 'HIGH'   THEN 1
					WHEN 'MEDIUM' THEN 2
					WHEN 'LOW'    THEN 3
				END,
				created_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING
			id, request_id, tenant_id, status, priority,
			payload, max_retries, retry_count, last_error,
			assigned_worker, lease_id, lease_expires_at,
			lease_version, run_timeout_ms,
			created_at, updated_at, dispatched_at,
			started_at, completed_at`

	row := s.db.Pool.QueryRow(ctx, query,
		workerID,
		fmt.Sprintf("%d seconds", int(leaseDuration.Seconds())),
	)

	job, err := scanJob(row)
	if err == pgx.ErrNoRows {
		return nil, nil // no jobs available, not an error
	}
	if err != nil {
		return nil, fmt.Errorf("failed to claim job: %w", err)
	}

	if err := s.recordTransition(ctx, job.ID, models.JobStatusPending, models.JobStatusDispatched, workerID, "claimed by worker"); err != nil {
		return nil, err
	}

	return job, nil
}

// MarkJobRunning transitions a job from DISPATCHED to RUNNING
func (s *JobStore) MarkJobRunning(ctx context.Context, jobID, workerID string, leaseVersion int) error {
	query := `
		UPDATE jobs SET
			status     = 'RUNNING',
			started_at = NOW()
		WHERE id            = $1
		  AND assigned_worker = $2
		  AND lease_version  = $3
		  AND status         = 'DISPATCHED'`

	result, err := s.db.Pool.Exec(ctx, query, jobID, workerID, leaseVersion)
	if err != nil {
		return fmt.Errorf("failed to mark job running: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("stale lease: job %s lease version mismatch", jobID)
	}

	return s.recordTransition(ctx, jobID, models.JobStatusDispatched, models.JobStatusRunning, workerID, "job started")
}

// CompleteJob marks a job as SUCCEEDED — validates lease version first
func (s *JobStore) CompleteJob(ctx context.Context, jobID, workerID string, leaseVersion int) error {
	query := `
		UPDATE jobs SET
			status       = 'SUCCEEDED',
			started_at   = COALESCE(started_at, NOW()),
			completed_at = NOW()
		WHERE id             = $1
		  AND assigned_worker = $2
		  AND lease_version   = $3
		  AND status IN ('RUNNING', 'DISPATCHED')`

	result, err := s.db.Pool.Exec(ctx, query, jobID, workerID, leaseVersion)
	if err != nil {
		return fmt.Errorf("failed to complete job: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("stale lease: job %s rejected completion from worker %s", jobID, workerID)
	}

	return s.recordTransition(ctx, jobID, models.JobStatusRunning, models.JobStatusSucceeded, workerID, "job completed")
}

// FailJob marks a job as FAILED and schedules retry if attempts remain
func (s *JobStore) FailJob(ctx context.Context, jobID, workerID, reason string, leaseVersion int) error {
	query := `
		UPDATE jobs SET
			status      = CASE
				WHEN retry_count + 1 >= max_retries THEN 'DEAD_LETTER'
				ELSE 'PENDING'
			END,
			retry_count     = retry_count + 1,
			last_error      = $4,
			assigned_worker = NULL,
			lease_id        = NULL,
			lease_expires_at = NULL
		WHERE id             = $1
		  AND assigned_worker = $2
		  AND lease_version   = $3
		  AND status IN ('RUNNING', 'DISPATCHED')
		RETURNING status, retry_count`

	var newStatus models.JobStatus
	var retryCount int
	err := s.db.Pool.QueryRow(ctx, query, jobID, workerID, leaseVersion, reason).
		Scan(&newStatus, &retryCount)
	if err == pgx.ErrNoRows {
		return fmt.Errorf("stale lease: job %s rejected failure from worker %s", jobID, workerID)
	}
	if err != nil {
		return fmt.Errorf("failed to fail job: %w", err)
	}

	return s.recordTransition(ctx, jobID, models.JobStatusRunning, newStatus, workerID, reason)
}

// ReclaimExpiredJobs finds jobs with expired leases and resets them to PENDING
// This is the reconciliation sweep — runs on startup and periodically
func (s *JobStore) ReclaimExpiredJobs(ctx context.Context) (int, error) {
	query := `
		UPDATE jobs SET
			status           = 'PENDING',
			assigned_worker  = NULL,
			lease_id         = NULL,
			lease_expires_at = NULL
		WHERE status IN ('DISPATCHED', 'RUNNING')
		  AND lease_expires_at < NOW()
		RETURNING id`

	rows, err := s.db.Pool.Query(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed to reclaim expired jobs: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var jobID string
		if err := rows.Scan(&jobID); err != nil {
			continue
		}
		s.recordTransition(ctx, jobID, models.JobStatusRunning, models.JobStatusPending, "", "lease expired — reclaimed")
		count++
	}

	return count, nil
}

// CancelJob cancels a job if it hasn't completed yet
func (s *JobStore) CancelJob(ctx context.Context, jobID, tenantID string) error {
	query := `
		UPDATE jobs SET
			status = 'CANCELLED'
		WHERE id        = $1
		  AND tenant_id = $2
		  AND status NOT IN ('SUCCEEDED', 'CANCELLED', 'DEAD_LETTER')`

	result, err := s.db.Pool.Exec(ctx, query, jobID, tenantID)
	if err != nil {
		return fmt.Errorf("failed to cancel job: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("job %s cannot be cancelled", jobID)
	}

	return s.recordTransition(ctx, jobID, "", models.JobStatusCancelled, "", "cancelled by client")
}

// ListJobs returns jobs for a tenant with optional status filter
func (s *JobStore) ListJobs(ctx context.Context, tenantID string, status *models.JobStatus, limit, offset int) ([]*models.Job, error) {
	query := `
		SELECT
			id, request_id, tenant_id, status, priority,
			payload, max_retries, retry_count, last_error,
			assigned_worker, lease_id, lease_expires_at,
			lease_version, run_timeout_ms,
			created_at, updated_at, dispatched_at,
			started_at, completed_at
		FROM jobs
		WHERE tenant_id = $1
		  AND ($2::text IS NULL OR status = $2)
		ORDER BY created_at DESC
		LIMIT $3 OFFSET $4`

	var statusVal interface{}
	if status != nil {
		statusVal = string(*status)
	}

	rows, err := s.db.Pool.Query(ctx, query, tenantID, statusVal, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*models.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

// recordTransition writes an audit log entry for every state change
func (s *JobStore) recordTransition(ctx context.Context, jobID string, from, to models.JobStatus, workerID, reason string) error {
	query := `
		INSERT INTO job_transitions (job_id, from_state, to_state, worker_id, reason)
		VALUES ($1, $2, $3, $4, $5)`

	_, err := s.db.Pool.Exec(ctx, query, jobID, string(from), string(to), workerID, reason)
	if err != nil {
		return fmt.Errorf("failed to record transition: %w", err)
	}
	return nil
}

// scanJob reads a database row into a Job struct
func scanJob(row pgx.Row) (*models.Job, error) {
	var job models.Job
	var runTimeoutMs int64
	var lastError, assignedWorker, leaseID *string
	var leaseExpiresAt *time.Time

	err := row.Scan(
		&job.ID,
		&job.RequestID,
		&job.TenantID,
		&job.Status,
		&job.Priority,
		&job.Payload,
		&job.MaxRetries,
		&job.RetryCount,
		&lastError,
		&assignedWorker,
		&leaseID,
		&leaseExpiresAt,
		&job.LeaseVersion,
		&runTimeoutMs,
		&job.CreatedAt,
		&job.UpdatedAt,
		&job.DispatchedAt,
		&job.StartedAt,
		&job.CompletedAt,
	)
	if err != nil {
		return nil, err
	}

	if lastError != nil {
		job.LastError = *lastError
	}
	if assignedWorker != nil {
		job.AssignedWorker = *assignedWorker
	}
	if leaseID != nil {
		job.LeaseID = *leaseID
	}
	if leaseExpiresAt != nil {
		job.LeaseExpiresAt = *leaseExpiresAt
	}

	job.RunTimeout = time.Duration(runTimeoutMs) * time.Millisecond
	return &job, nil
}
