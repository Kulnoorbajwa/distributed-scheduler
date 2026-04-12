package db

import (
	"context"
	"fmt"
	"time"

	"github.com/Kulnoorbajwa/distributed-scheduler/internal/models"
	"github.com/jackc/pgx/v5"
)

// ScheduleStore handles all schedule-related database operations
type ScheduleStore struct {
	db *DB
}

// NewScheduleStore creates a new ScheduleStore
func NewScheduleStore(db *DB) *ScheduleStore {
	return &ScheduleStore{db: db}
}

// CreateSchedule inserts a new schedule after validating tenant quota
func (s *ScheduleStore) CreateSchedule(ctx context.Context, sched *models.Schedule) (*models.Schedule, error) {
	// Check per-tenant schedule quota
	var count int
	err := s.db.Pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM schedules WHERE tenant_id = $1`, sched.TenantID,
	).Scan(&count)
	if err != nil {
		return nil, fmt.Errorf("failed to check schedule count: %w", err)
	}

	var maxSchedules int
	err = s.db.Pool.QueryRow(ctx,
		`SELECT COALESCE(
			(SELECT max_schedules FROM tenant_quotas WHERE tenant_id = $1),
			50
		)`, sched.TenantID,
	).Scan(&maxSchedules)
	if err != nil {
		return nil, fmt.Errorf("failed to check schedule quota: %w", err)
	}

	if count >= maxSchedules {
		return nil, fmt.Errorf("tenant %q has reached maximum schedule quota (%d)", sched.TenantID, maxSchedules)
	}

	query := `
		INSERT INTO schedules (
			tenant_id, name, cron_expr, payload, priority,
			max_retries, run_timeout_ms, enabled, missed_policy, next_run_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10
		)
		RETURNING id, created_at, updated_at`

	err = s.db.Pool.QueryRow(ctx, query,
		sched.TenantID,
		sched.Name,
		sched.CronExpr,
		sched.Payload,
		string(sched.Priority),
		sched.MaxRetries,
		sched.RunTimeout.Milliseconds(),
		sched.Enabled,
		string(sched.MissedPolicy),
		sched.NextRunAt,
	).Scan(&sched.ID, &sched.CreatedAt, &sched.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to create schedule: %w", err)
	}

	return sched, nil
}

// GetDueSchedules returns schedules whose next_run_at <= now and are enabled.
// Uses FOR UPDATE SKIP LOCKED to prevent double-firing in HA setups.
func (s *ScheduleStore) GetDueSchedules(ctx context.Context) ([]*models.Schedule, error) {
	query := `
		SELECT
			id, tenant_id, name, cron_expr, payload, priority,
			max_retries, run_timeout_ms, enabled, missed_policy,
			last_run_at, next_run_at, created_at, updated_at
		FROM schedules
		WHERE enabled = TRUE
		  AND next_run_at <= NOW()
		ORDER BY next_run_at ASC
		FOR UPDATE SKIP LOCKED`

	rows, err := s.db.Pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get due schedules: %w", err)
	}
	defer rows.Close()

	var schedules []*models.Schedule
	for rows.Next() {
		sched, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		schedules = append(schedules, sched)
	}
	return schedules, nil
}

// AdvanceSchedule updates last_run_at and next_run_at after firing
func (s *ScheduleStore) AdvanceSchedule(ctx context.Context, scheduleID string, lastRun, nextRun time.Time) error {
	query := `
		UPDATE schedules SET
			last_run_at = $2,
			next_run_at = $3
		WHERE id = $1`

	_, err := s.db.Pool.Exec(ctx, query, scheduleID, lastRun, nextRun)
	if err != nil {
		return fmt.Errorf("failed to advance schedule: %w", err)
	}
	return nil
}

// ListSchedules returns all schedules for a tenant
func (s *ScheduleStore) ListSchedules(ctx context.Context, tenantID string) ([]*models.Schedule, error) {
	query := `
		SELECT
			id, tenant_id, name, cron_expr, payload, priority,
			max_retries, run_timeout_ms, enabled, missed_policy,
			last_run_at, next_run_at, created_at, updated_at
		FROM schedules
		WHERE tenant_id = $1
		ORDER BY created_at DESC`

	rows, err := s.db.Pool.Query(ctx, query, tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to list schedules: %w", err)
	}
	defer rows.Close()

	var schedules []*models.Schedule
	for rows.Next() {
		sched, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		schedules = append(schedules, sched)
	}
	return schedules, nil
}

// ToggleSchedule enables or disables a schedule (tenant-scoped)
func (s *ScheduleStore) ToggleSchedule(ctx context.Context, scheduleID, tenantID string, enabled bool) error {
	query := `
		UPDATE schedules SET enabled = $3
		WHERE id = $1 AND tenant_id = $2`

	result, err := s.db.Pool.Exec(ctx, query, scheduleID, tenantID, enabled)
	if err != nil {
		return fmt.Errorf("failed to toggle schedule: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("schedule %s not found for tenant %s", scheduleID, tenantID)
	}
	return nil
}

// DeleteSchedule removes a schedule (tenant-scoped)
func (s *ScheduleStore) DeleteSchedule(ctx context.Context, scheduleID, tenantID string) error {
	query := `DELETE FROM schedules WHERE id = $1 AND tenant_id = $2`

	result, err := s.db.Pool.Exec(ctx, query, scheduleID, tenantID)
	if err != nil {
		return fmt.Errorf("failed to delete schedule: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("schedule %s not found for tenant %s", scheduleID, tenantID)
	}
	return nil
}

// GetScheduleByName fetches a schedule by tenant + name
func (s *ScheduleStore) GetScheduleByName(ctx context.Context, tenantID, name string) (*models.Schedule, error) {
	query := `
		SELECT
			id, tenant_id, name, cron_expr, payload, priority,
			max_retries, run_timeout_ms, enabled, missed_policy,
			last_run_at, next_run_at, created_at, updated_at
		FROM schedules
		WHERE tenant_id = $1 AND name = $2`

	row := s.db.Pool.QueryRow(ctx, query, tenantID, name)
	sched, err := scanSchedule(row)
	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("schedule %q not found for tenant %s", name, tenantID)
	}
	return sched, err
}

func scanSchedule(row pgx.Row) (*models.Schedule, error) {
	var sched models.Schedule
	var runTimeoutMs int64

	err := row.Scan(
		&sched.ID,
		&sched.TenantID,
		&sched.Name,
		&sched.CronExpr,
		&sched.Payload,
		&sched.Priority,
		&sched.MaxRetries,
		&runTimeoutMs,
		&sched.Enabled,
		&sched.MissedPolicy,
		&sched.LastRunAt,
		&sched.NextRunAt,
		&sched.CreatedAt,
		&sched.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	sched.RunTimeout = time.Duration(runTimeoutMs) * time.Millisecond
	return &sched, nil
}
