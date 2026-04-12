package autopsy

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// AutopsyReport is the forensic analysis generated when a job hits DEAD_LETTER
type AutopsyReport struct {
	JobID           string          `json:"job_id"`
	TenantID        string          `json:"tenant_id"`
	Payload         string          `json:"payload"`
	Priority        string          `json:"priority"`
	TotalAttempts   int             `json:"total_attempts"`
	TotalElapsed    string          `json:"total_elapsed"`
	Attempts        []AttemptDetail `json:"attempts"`
	Timeline        []TimelineEvent `json:"timeline"`
	ProbableCause   string          `json:"probable_cause"`
	Recommendations []string        `json:"recommendations"`
	GeneratedAt     time.Time       `json:"generated_at"`
}

// AttemptDetail captures one execution attempt
type AttemptDetail struct {
	Attempt  int    `json:"attempt"`
	WorkerID string `json:"worker_id"`
	Error    string `json:"error"`
	// Timestamps from transitions
	StartedAt string `json:"started_at,omitempty"`
	FailedAt  string `json:"failed_at,omitempty"`
}

// TimelineEvent is a single event in the job's lifecycle
type TimelineEvent struct {
	Timestamp string `json:"timestamp"`
	Event     string `json:"event"`
	WorkerID  string `json:"worker_id,omitempty"`
}

// transitionRow is the raw DB row from job_transitions
type transitionRow struct {
	FromState string
	ToState   string
	WorkerID  string
	Reason    string
	CreatedAt time.Time
}

// GenerateAutopsy builds a forensic report for a dead-lettered job.
// It queries job data and transition history, scrubs secrets, and classifies the failure.
func GenerateAutopsy(ctx context.Context, pool *pgxpool.Pool, jobID string) (*AutopsyReport, error) {
	// Fetch job details
	var tenantID, payload, priority string
	var retryCount int
	err := pool.QueryRow(ctx,
		`SELECT tenant_id, payload, priority, retry_count FROM jobs WHERE id = $1`,
		jobID,
	).Scan(&tenantID, &payload, &priority, &retryCount)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch job: %w", err)
	}

	// Fetch transitions (capped at 100 to prevent unbounded report size)
	rows, err := pool.Query(ctx,
		`SELECT from_state, to_state, COALESCE(worker_id, ''), COALESCE(reason, ''), created_at
		 FROM job_transitions WHERE job_id = $1 ORDER BY created_at ASC LIMIT 100`,
		jobID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch transitions: %w", err)
	}
	defer rows.Close()

	var transitions []transitionRow
	for rows.Next() {
		var t transitionRow
		if err := rows.Scan(&t.FromState, &t.ToState, &t.WorkerID, &t.Reason, &t.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan transition: %w", err)
		}
		transitions = append(transitions, t)
	}

	// Build timeline
	var timeline []TimelineEvent
	for _, t := range transitions {
		event := fmt.Sprintf("%s → %s", t.FromState, t.ToState)
		if t.Reason != "" {
			event += ": " + ScrubSecrets(t.Reason)
		}
		timeline = append(timeline, TimelineEvent{
			Timestamp: t.CreatedAt.Format(time.RFC3339),
			Event:     event,
			WorkerID:  t.WorkerID,
		})
	}

	// Build attempt details from failure transitions
	var attempts []AttemptDetail
	attemptNum := 0
	for _, t := range transitions {
		if t.ToState == "PENDING" && t.FromState != "" || t.ToState == "DEAD_LETTER" {
			attemptNum++
			attempts = append(attempts, AttemptDetail{
				Attempt:  attemptNum,
				WorkerID: t.WorkerID,
				Error:    ScrubSecrets(t.Reason),
				FailedAt: t.CreatedAt.Format(time.RFC3339),
			})
		}
	}

	// Compute total elapsed time
	var totalElapsed string
	if len(transitions) >= 2 {
		elapsed := transitions[len(transitions)-1].CreatedAt.Sub(transitions[0].CreatedAt)
		totalElapsed = elapsed.String()
	}

	// Classify probable cause
	cause, recommendations := classifyCause(attempts, transitions)

	report := &AutopsyReport{
		JobID:           jobID,
		TenantID:        tenantID,
		Payload:         ScrubSecrets(payload),
		Priority:        priority,
		TotalAttempts:   retryCount,
		TotalElapsed:    totalElapsed,
		Attempts:        attempts,
		Timeline:        timeline,
		ProbableCause:   cause,
		Recommendations: recommendations,
		GeneratedAt:     time.Now(),
	}

	return report, nil
}

// classifyCause analyzes the failure patterns and returns a cause + recommendations
func classifyCause(attempts []AttemptDetail, transitions []transitionRow) (string, []string) {
	if len(attempts) == 0 {
		return "unknown — no failure data available", nil
	}

	// Collect error messages
	var errors []string
	var workers []string
	for _, a := range attempts {
		errors = append(errors, a.Error)
		workers = append(workers, a.WorkerID)
	}

	// Check for timeout pattern
	allTimeout := true
	for _, e := range errors {
		if !strings.Contains(strings.ToLower(e), "timeout") &&
			!strings.Contains(strings.ToLower(e), "timed out") &&
			!strings.Contains(strings.ToLower(e), "deadline exceeded") {
			allTimeout = false
			break
		}
	}
	if allTimeout {
		return "job consistently exceeds run timeout",
			[]string{"Increase run_timeout_ms", "Optimize the job payload", "Check target service responsiveness"}
	}

	// Check for connection errors
	allConnErr := true
	for _, e := range errors {
		if !strings.Contains(strings.ToLower(e), "connection refused") &&
			!strings.Contains(strings.ToLower(e), "connection reset") &&
			!strings.Contains(strings.ToLower(e), "no such host") {
			allConnErr = false
			break
		}
	}
	if allConnErr {
		return "target service unavailable — all attempts failed with connection errors",
			[]string{"Verify the target URL/host is reachable", "Check network configuration", "Ensure the target service is running"}
	}

	// Check for command rejection
	allRejected := true
	for _, e := range errors {
		if !strings.Contains(strings.ToLower(e), "command rejected") &&
			!strings.Contains(strings.ToLower(e), "not in the allowlist") {
			allRejected = false
			break
		}
	}
	if allRejected {
		return "payload validation failure — job will never succeed with current payload",
			[]string{"Check that the command is in the allowlist", "Verify the payload format is correct"}
	}

	// Check if all errors are identical
	allSame := true
	for _, e := range errors[1:] {
		if e != errors[0] {
			allSame = false
			break
		}
	}
	if allSame {
		return "persistent failure — same error on every attempt: " + errors[0],
			[]string{"Fix the underlying cause before retrying", "Check job payload for correctness"}
	}

	// Check if all on same worker
	allSameWorker := true
	for _, w := range workers[1:] {
		if w != workers[0] {
			allSameWorker = false
			break
		}
	}
	if allSameWorker && workers[0] != "" {
		return fmt.Sprintf("worker-specific issue — all attempts ran on worker %s", workers[0]),
			[]string{"Check worker health and resources", "Try resubmitting to allow different worker assignment"}
	}

	return "intermittent failure — errors vary across attempts (possibly flaky)",
		[]string{"Review error messages for patterns", "Check for resource contention", "Consider increasing max_retries"}
}

// StoreAutopsy persists an autopsy report to the database
func StoreAutopsy(ctx context.Context, pool *pgxpool.Pool, report *AutopsyReport) error {
	reportJSON, err := json.Marshal(report)
	if err != nil {
		return fmt.Errorf("failed to marshal autopsy report: %w", err)
	}

	_, err = pool.Exec(ctx,
		`INSERT INTO autopsy_reports (job_id, tenant_id, report) VALUES ($1, $2, $3)`,
		report.JobID, report.TenantID, reportJSON,
	)
	if err != nil {
		return fmt.Errorf("failed to store autopsy report: %w", err)
	}

	return nil
}
