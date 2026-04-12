package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// AutopsyStore handles autopsy report database operations
type AutopsyStore struct {
	db *DB
}

// NewAutopsyStore creates a new AutopsyStore
func NewAutopsyStore(db *DB) *AutopsyStore {
	return &AutopsyStore{db: db}
}

// StoredAutopsy is the report as stored in the database
type StoredAutopsy struct {
	ID        string
	JobID     string
	TenantID  string
	Report    json.RawMessage
	CreatedAt time.Time
}

// GetAutopsyByJobID fetches the autopsy report for a job (tenant-scoped for security)
func (s *AutopsyStore) GetAutopsyByJobID(ctx context.Context, jobID, tenantID string) (*StoredAutopsy, error) {
	query := `
		SELECT id, job_id, tenant_id, report, created_at
		FROM autopsy_reports
		WHERE job_id = $1 AND tenant_id = $2`

	var a StoredAutopsy
	err := s.db.Pool.QueryRow(ctx, query, jobID, tenantID).
		Scan(&a.ID, &a.JobID, &a.TenantID, &a.Report, &a.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("no autopsy found for job %s", jobID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to fetch autopsy: %w", err)
	}
	return &a, nil
}

// ListAutopsies returns recent autopsy reports for a tenant
func (s *AutopsyStore) ListAutopsies(ctx context.Context, tenantID string, limit int) ([]*StoredAutopsy, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	query := `
		SELECT id, job_id, tenant_id, report, created_at
		FROM autopsy_reports
		WHERE tenant_id = $1
		ORDER BY created_at DESC
		LIMIT $2`

	rows, err := s.db.Pool.Query(ctx, query, tenantID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list autopsies: %w", err)
	}
	defer rows.Close()

	var autopsies []*StoredAutopsy
	for rows.Next() {
		var a StoredAutopsy
		if err := rows.Scan(&a.ID, &a.JobID, &a.TenantID, &a.Report, &a.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan autopsy: %w", err)
		}
		autopsies = append(autopsies, &a)
	}
	return autopsies, nil
}
