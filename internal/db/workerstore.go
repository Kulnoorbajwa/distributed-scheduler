package db

import (
	"context"
	"fmt"
	"time"

	"github.com/Kulnoorbajwa/distributed-scheduler/internal/models"
	"github.com/jackc/pgx/v5"
)

// WorkerStore handles all worker-related database operations
type WorkerStore struct {
	db *DB
}

// NewWorkerStore creates a new WorkerStore
func NewWorkerStore(db *DB) *WorkerStore {
	return &WorkerStore{db: db}
}

// RegisterWorker inserts a new worker or updates it if it already exists
// Workers re-register on restart so this must be idempotent
func (s *WorkerStore) RegisterWorker(ctx context.Context, worker *models.Worker) (*models.Worker, error) {
	query := `
		INSERT INTO workers (
			id, tenant_id, status, address,
			max_jobs, cpu_slots, memory_mb, version
		) VALUES (
			$1, $2, 'ACTIVE', $3, $4, $5, $6, $7
		)
		ON CONFLICT (id) DO UPDATE SET
			status         = 'ACTIVE',
			address        = EXCLUDED.address,
			last_heartbeat = NOW(),
			version        = EXCLUDED.version
		RETURNING id, status, registered_at, last_heartbeat`

	err := s.db.Pool.QueryRow(ctx, query,
		worker.ID,
		worker.TenantID,
		worker.Address,
		worker.MaxJobs,
		worker.CPUSlots,
		worker.MemoryMB,
		worker.Version,
	).Scan(
		&worker.ID,
		&worker.Status,
		&worker.RegisteredAt,
		&worker.LastHeartbeat,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register worker: %w", err)
	}

	return worker, nil
}

// RecordHeartbeat updates a worker's last_heartbeat timestamp
// Called every HeartbeatInterval by each worker
func (s *WorkerStore) RecordHeartbeat(ctx context.Context, workerID string, activeJobs int) error {
	query := `
		UPDATE workers SET
			last_heartbeat = NOW(),
			active_jobs    = $2
		WHERE id     = $1
		  AND status = 'ACTIVE'`

	result, err := s.db.Pool.Exec(ctx, query, workerID, activeJobs)
	if err != nil {
		return fmt.Errorf("failed to record heartbeat: %w", err)
	}
	if result.RowsAffected() == 0 {
		// Worker was marked dead — needs to re-register
		return fmt.Errorf("worker %s not found or not active — re-registration required", workerID)
	}

	return nil
}

// MarkWorkerDead marks a worker as dead when heartbeat times out
func (s *WorkerStore) MarkWorkerDead(ctx context.Context, workerID string) error {
	query := `
		UPDATE workers SET
			status = 'DEAD'
		WHERE id     = $1
		  AND status = 'ACTIVE'`

	_, err := s.db.Pool.Exec(ctx, query, workerID)
	if err != nil {
		return fmt.Errorf("failed to mark worker dead: %w", err)
	}

	return nil
}

// GetWorker fetches a single worker by ID
func (s *WorkerStore) GetWorker(ctx context.Context, workerID string) (*models.Worker, error) {
	query := `
		SELECT
			id, tenant_id, status, address,
			last_heartbeat, registered_at,
			active_jobs, max_jobs,
			cpu_slots, memory_mb, version
		FROM workers WHERE id = $1`

	row := s.db.Pool.QueryRow(ctx, query, workerID)
	return scanWorker(row)
}

// GetActiveWorkers returns all workers currently marked ACTIVE
func (s *WorkerStore) GetActiveWorkers(ctx context.Context) ([]*models.Worker, error) {
	query := `
		SELECT
			id, tenant_id, status, address,
			last_heartbeat, registered_at,
			active_jobs, max_jobs,
			cpu_slots, memory_mb, version
		FROM workers
		WHERE status = 'ACTIVE'
		ORDER BY active_jobs ASC` // least loaded first

	rows, err := s.db.Pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get active workers: %w", err)
	}
	defer rows.Close()

	var workers []*models.Worker
	for rows.Next() {
		worker, err := scanWorker(rows)
		if err != nil {
			return nil, err
		}
		workers = append(workers, worker)
	}

	return workers, nil
}

// GetStaledWorkers returns workers whose heartbeat has timed out
// The scheduler calls this periodically to reap dead workers
func (s *WorkerStore) GetStaleWorkers(ctx context.Context, timeout time.Duration) ([]*models.Worker, error) {
	query := `
		SELECT
			id, tenant_id, status, address,
			last_heartbeat, registered_at,
			active_jobs, max_jobs,
			cpu_slots, memory_mb, version
		FROM workers
		WHERE status         = 'ACTIVE'
		  AND last_heartbeat < NOW() - $1::interval`

	rows, err := s.db.Pool.Query(ctx,
		query,
		fmt.Sprintf("%d seconds", int(timeout.Seconds())),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get stale workers: %w", err)
	}
	defer rows.Close()

	var workers []*models.Worker
	for rows.Next() {
		worker, err := scanWorker(rows)
		if err != nil {
			return nil, err
		}
		workers = append(workers, worker)
	}

	return workers, nil
}

// DrainWorker sets a worker to DRAINING for graceful shutdown
// A draining worker finishes current jobs but accepts no new ones
func (s *WorkerStore) DrainWorker(ctx context.Context, workerID string) error {
	query := `
		UPDATE workers SET
			status = 'DRAINING'
		WHERE id     = $1
		  AND status = 'ACTIVE'`

	_, err := s.db.Pool.Exec(ctx, query, workerID)
	if err != nil {
		return fmt.Errorf("failed to drain worker: %w", err)
	}

	return nil
}

// GetLeastLoadedWorker returns the active worker with the most available capacity
// Used by the scheduler for load balancing
func (s *WorkerStore) GetLeastLoadedWorker(ctx context.Context) (*models.Worker, error) {
	query := `
		SELECT
			id, tenant_id, status, address,
			last_heartbeat, registered_at,
			active_jobs, max_jobs,
			cpu_slots, memory_mb, version
		FROM workers
		WHERE status      = 'ACTIVE'
		  AND active_jobs < max_jobs
		ORDER BY
			(active_jobs::float / max_jobs::float) ASC
		LIMIT 1`

	row := s.db.Pool.QueryRow(ctx, query)
	worker, err := scanWorker(row)
	if err == pgx.ErrNoRows {
		return nil, nil // no available workers
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get least loaded worker: %w", err)
	}

	return worker, nil
}

// scanWorker reads a database row into a Worker struct
func scanWorker(row pgx.Row) (*models.Worker, error) {
	var worker models.Worker

	err := row.Scan(
		&worker.ID,
		&worker.TenantID,
		&worker.Status,
		&worker.Address,
		&worker.LastHeartbeat,
		&worker.RegisteredAt,
		&worker.ActiveJobs,
		&worker.MaxJobs,
		&worker.CPUSlots,
		&worker.MemoryMB,
		&worker.Version,
	)
	if err != nil {
		return nil, err
	}

	return &worker, nil
}
