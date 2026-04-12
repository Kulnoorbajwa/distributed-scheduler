package main

import (
	"context"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	"go.uber.org/zap"

	"github.com/Kulnoorbajwa/distributed-scheduler/internal/db"
	"github.com/Kulnoorbajwa/distributed-scheduler/internal/models"
)

// cronEvaluator checks for due schedules and fires jobs
type cronEvaluator struct {
	scheduleStore *db.ScheduleStore
	jobStore      *db.JobStore
	log           *zap.Logger
	parser        cron.Parser
}

func newCronEvaluator(scheduleStore *db.ScheduleStore, jobStore *db.JobStore, log *zap.Logger) *cronEvaluator {
	return &cronEvaluator{
		scheduleStore: scheduleStore,
		jobStore:      jobStore,
		log:           log,
		parser:        cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow),
	}
}

// ValidateCronExpr validates a cron expression and rejects sub-minute intervals.
// Returns an error if invalid or fires too frequently.
func ValidateCronExpr(expr string) error {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser.Parse(expr)
	if err != nil {
		return fmt.Errorf("invalid cron expression %q: %w", expr, err)
	}

	// Check minimum interval: reject anything that fires more often than once per minute
	now := time.Now()
	next1 := sched.Next(now)
	next2 := sched.Next(next1)
	interval := next2.Sub(next1)
	if interval < 1*time.Minute {
		return fmt.Errorf("cron expression %q fires every %v — minimum interval is 1 minute", expr, interval)
	}

	return nil
}

// nextRunTime computes the next run time for a cron expression after the given time
func (ce *cronEvaluator) nextRunTime(cronExpr string, after time.Time) (time.Time, error) {
	sched, err := ce.parser.Parse(cronExpr)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid cron expression: %w", err)
	}
	return sched.Next(after), nil
}

// run is the main loop — called as a goroutine from the scheduler
func (ce *cronEvaluator) run(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Run once immediately on startup
	ce.evaluate(ctx)

	for {
		select {
		case <-ctx.Done():
			ce.log.Info("cron evaluator stopped")
			return
		case <-ticker.C:
			ce.evaluate(ctx)
		}
	}
}

// evaluate checks for due schedules and fires them
func (ce *cronEvaluator) evaluate(ctx context.Context) {
	schedules, err := ce.scheduleStore.GetDueSchedules(ctx)
	if err != nil {
		ce.log.Error("failed to get due schedules", zap.Error(err))
		return
	}

	for _, sched := range schedules {
		ce.fireSchedule(ctx, sched)
	}
}

// fireSchedule creates a job for a due schedule and advances it
func (ce *cronEvaluator) fireSchedule(ctx context.Context, sched *models.Schedule) {
	now := time.Now()

	// Handle missed runs based on policy
	runsToFire := ce.computeMissedRuns(sched, now)

	for _, runTime := range runsToFire {
		// Generate idempotent request_id from schedule ID + run time
		requestID := fmt.Sprintf("schedule:%s:%d", sched.ID, runTime.Unix())

		job := &models.Job{
			RequestID:  requestID,
			TenantID:   sched.TenantID,
			Priority:   sched.Priority,
			Payload:    sched.Payload,
			MaxRetries: sched.MaxRetries,
			RunTimeout: sched.RunTimeout,
		}

		created, err := ce.jobStore.CreateJob(ctx, job)
		if err != nil {
			ce.log.Error("failed to create scheduled job",
				zap.String("schedule_id", sched.ID),
				zap.String("schedule_name", sched.Name),
				zap.Error(err),
			)
			return
		}

		ce.log.Info("scheduled job fired",
			zap.String("schedule_id", sched.ID),
			zap.String("schedule_name", sched.Name),
			zap.String("job_id", created.ID),
			zap.Time("run_time", runTime),
		)
	}

	// Advance to next future run
	nextRun, err := ce.nextRunTime(sched.CronExpr, now)
	if err != nil {
		ce.log.Error("failed to compute next run time",
			zap.String("schedule_id", sched.ID),
			zap.Error(err),
		)
		return
	}

	if err := ce.scheduleStore.AdvanceSchedule(ctx, sched.ID, now, nextRun); err != nil {
		ce.log.Error("failed to advance schedule",
			zap.String("schedule_id", sched.ID),
			zap.Error(err),
		)
	}
}

// computeMissedRuns determines which runs to fire based on the missed-run policy
func (ce *cronEvaluator) computeMissedRuns(sched *models.Schedule, now time.Time) []time.Time {
	switch sched.MissedPolicy {
	case models.MissedRunPolicySkip:
		// Just fire once for the current due time
		return []time.Time{sched.NextRunAt}

	case models.MissedRunPolicyRunOnce:
		// Fire once regardless of how many were missed
		return []time.Time{sched.NextRunAt}

	case models.MissedRunPolicyRunAll:
		// Fire for each missed interval, capped at 10 to prevent schedule bombs
		var runs []time.Time
		runTime := sched.NextRunAt
		maxCatchup := 10

		for runTime.Before(now) || runTime.Equal(now) {
			runs = append(runs, runTime)
			if len(runs) >= maxCatchup {
				ce.log.Warn("capped missed runs at maximum",
					zap.String("schedule_id", sched.ID),
					zap.Int("max", maxCatchup),
				)
				break
			}
			next, err := ce.nextRunTime(sched.CronExpr, runTime)
			if err != nil {
				break
			}
			runTime = next
		}
		return runs

	default:
		return []time.Time{sched.NextRunAt}
	}
}
