package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Kulnoorbajwa/distributed-scheduler/config"
	"github.com/Kulnoorbajwa/distributed-scheduler/internal/autopsy"
	"github.com/Kulnoorbajwa/distributed-scheduler/internal/db"
	"github.com/Kulnoorbajwa/distributed-scheduler/internal/models"
	pb "github.com/Kulnoorbajwa/distributed-scheduler/proto"
)

// Scheduler is the master node — it holds all the state
// and coordinates workers
type Scheduler struct {
	pb.UnimplementedSchedulerServiceServer
	cfg           *config.Config
	db            *db.DB
	jobStore      *db.JobStore
	workerStore   *db.WorkerStore
	scheduleStore *db.ScheduleStore
	log           *zap.Logger
}

// NewScheduler creates a new scheduler instance
func NewScheduler(cfg *config.Config, database *db.DB, log *zap.Logger) *Scheduler {
	return &Scheduler{
		cfg:           cfg,
		db:            database,
		jobStore:      db.NewJobStore(database),
		workerStore:   db.NewWorkerStore(database),
		scheduleStore: db.NewScheduleStore(database),
		log:           log,
	}
}

// ─────────────────────────────────────────
// Worker-facing RPC handlers
// ─────────────────────────────────────────

// RegisterWorker handles a worker coming online
func (s *Scheduler) RegisterWorker(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	s.log.Info("worker registering",
		zap.String("worker_id", req.Worker.Id),
		zap.String("address", req.Worker.Address),
	)

	worker := &models.Worker{
		ID:       req.Worker.Id,
		TenantID: req.Worker.TenantId,
		Address:  req.Worker.Address,
		MaxJobs:  int(req.Worker.MaxJobs),
		CPUSlots: int(req.Worker.CpuSlots),
		MemoryMB: int(req.Worker.MemoryMb),
		Version:  req.Worker.Version,
	}

	_, err := s.workerStore.RegisterWorker(ctx, worker)
	if err != nil {
		s.log.Error("failed to register worker", zap.Error(err))
		return &pb.RegisterResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	s.log.Info("worker registered successfully", zap.String("worker_id", worker.ID))
	return &pb.RegisterResponse{
		Success:  true,
		WorkerId: worker.ID,
		Message:  "registered",
	}, nil
}

// Heartbeat handles a worker's liveness ping
func (s *Scheduler) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	// If worker is draining, mark it so the dispatcher won't send new jobs
	if req.Status == "DRAINING" {
		if err := s.workerStore.DrainWorker(ctx, req.WorkerId); err != nil {
			s.log.Warn("failed to drain worker", zap.String("worker_id", req.WorkerId), zap.Error(err))
		} else {
			s.log.Info("worker marked as DRAINING", zap.String("worker_id", req.WorkerId))
		}
		return &pb.HeartbeatResponse{Success: true, Message: "DRAINING_ACK"}, nil
	}

	err := s.workerStore.RecordHeartbeat(ctx, req.WorkerId, int(req.ActiveJobs))
	if err != nil {
		// Worker was marked dead — tell it to re-register
		s.log.Warn("heartbeat from unknown or dead worker",
			zap.String("worker_id", req.WorkerId),
		)
		return &pb.HeartbeatResponse{
			Success: false,
			Message: "RE_REGISTER",
		}, nil
	}

	return &pb.HeartbeatResponse{Success: true, Message: "OK"}, nil
}

// ReportResult handles a worker reporting job completion or failure
func (s *Scheduler) ReportResult(ctx context.Context, req *pb.JobResultRequest) (*pb.JobResultResponse, error) {
	s.log.Info("job result received",
		zap.String("job_id", req.JobId),
		zap.String("worker_id", req.WorkerId),
		zap.Bool("success", req.Success),
		zap.Int("lease_version", int(req.LeaseVersion)),
	)

	var err error
	if req.Success {
		err = s.jobStore.CompleteJob(ctx, req.JobId, req.WorkerId, int(req.LeaseVersion))
		if err == nil {
			jobsCompleted.WithLabelValues("succeeded").Inc()
		}
	} else {
		err = s.jobStore.FailJob(ctx, req.JobId, req.WorkerId, req.ErrorMessage, int(req.LeaseVersion), s.cfg.RetryBaseDelay, s.cfg.RetryMaxDelay)
		if err == nil {
			jobsCompleted.WithLabelValues("failed").Inc()

			// Check if the job moved to DEAD_LETTER — if so, generate autopsy
			job, jobErr := s.jobStore.GetJobByID(ctx, req.JobId)
			if jobErr == nil && job.Status == models.JobStatusDeadLetter {
				go s.generateAutopsy(req.JobId)
			}
		}
	}

	if err != nil {
		// Stale lease — worker was marked dead and reassigned
		s.log.Warn("rejected stale job result",
			zap.String("job_id", req.JobId),
			zap.String("worker_id", req.WorkerId),
			zap.Error(err),
		)
		return &pb.JobResultResponse{
			Accepted: false,
			Message:  err.Error(),
		}, nil
	}

	return &pb.JobResultResponse{Accepted: true, Message: "accepted"}, nil
}

// RenewLease extends a job's lease so long-running jobs don't get reaped
func (s *Scheduler) RenewLease(ctx context.Context, req *pb.LeaseRenewRequest) (*pb.LeaseRenewResponse, error) {
	query := `
		UPDATE jobs SET
			lease_expires_at = NOW() + $3::interval
		WHERE id             = $1
		  AND assigned_worker = $2
		  AND lease_version   = $4
		  AND status          = 'RUNNING'
		RETURNING lease_expires_at`

	newExpiry := time.Now()
	interval := fmt.Sprintf("%d seconds", int(s.cfg.LeaseDuration.Seconds()))

	err := s.db.Pool.QueryRow(ctx, query,
		req.JobId,
		req.WorkerId,
		interval,
		req.LeaseVersion,
	).Scan(&newExpiry)

	if err != nil {
		leaseRejections.Inc()
		return &pb.LeaseRenewResponse{
			Success: false,
			Message: "stale lease — job may have been reassigned",
		}, nil
	}

	leaseRenewals.Inc()
	return &pb.LeaseRenewResponse{
		Success:     true,
		NewExpiryMs: newExpiry.UnixMilli(),
		Message:     "renewed",
	}, nil
}

// ─────────────────────────────────────────
// Client-facing RPC handlers
// ─────────────────────────────────────────

// SubmitJob handles a client submitting a new job
func (s *Scheduler) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	// Validate payload size
	if len(req.Payload) > s.cfg.MaxPayloadBytes {
		return nil, status.Errorf(codes.InvalidArgument, "payload exceeds max size of %d bytes", s.cfg.MaxPayloadBytes)
	}

	// Check tenant quota
	count, err := s.getTenantJobCount(ctx, req.TenantId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check tenant quota")
	}
	if count >= s.cfg.DefaultTenantQuota {
		return nil, status.Errorf(codes.ResourceExhausted, "tenant %s has reached job quota", req.TenantId)
	}

	job := &models.Job{
		RequestID:  req.RequestId,
		TenantID:   req.TenantId,
		Priority:   models.JobPriority(req.Priority),
		Payload:    req.Payload,
		MaxRetries: int(req.MaxRetries),
		RunTimeout: time.Duration(req.RunTimeoutMs) * time.Millisecond,
	}

	created, err := s.jobStore.CreateJob(ctx, job)
	if err != nil {
		s.log.Error("failed to create job", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to create job")
	}

	jobsSubmitted.WithLabelValues(req.Priority).Inc()

	s.log.Info("job submitted",
		zap.String("job_id", created.ID),
		zap.String("tenant_id", req.TenantId),
		zap.String("priority", req.Priority),
	)

	return &pb.SubmitJobResponse{
		Success: true,
		JobId:   created.ID,
		Message: "job queued",
	}, nil
}

// GetJob returns the current status of a job
func (s *Scheduler) GetJob(ctx context.Context, req *pb.GetJobRequest) (*pb.GetJobResponse, error) {
	job, err := s.jobStore.GetJobByID(ctx, req.JobId)
	if err != nil {
		return &pb.GetJobResponse{Found: false, Message: "job not found"}, nil
	}

	return &pb.GetJobResponse{
		Found: true,
		Job: &pb.Job{
			Id:           job.ID,
			RequestId:    job.RequestID,
			TenantId:     job.TenantID,
			Status:       string(job.Status),
			Priority:     string(job.Priority),
			Payload:      job.Payload,
			MaxRetries:   int32(job.MaxRetries),
			RetryCount:   int32(job.RetryCount),
			LastError:    job.LastError,
			LeaseVersion: int32(job.LeaseVersion),
		},
	}, nil
}

// CancelJob cancels a job that hasn't completed yet
func (s *Scheduler) CancelJob(ctx context.Context, req *pb.CancelJobClientRequest) (*pb.CancelJobClientResponse, error) {
	err := s.jobStore.CancelJob(ctx, req.JobId, req.TenantId)
	if err != nil {
		return &pb.CancelJobClientResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	s.log.Info("job cancelled",
		zap.String("job_id", req.JobId),
		zap.String("tenant_id", req.TenantId),
	)

	return &pb.CancelJobClientResponse{Success: true, Message: "cancelled"}, nil
}

// ListJobs returns jobs for a tenant
func (s *Scheduler) ListJobs(ctx context.Context, req *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
	limit := int(req.Limit)
	if limit == 0 {
		limit = 20
	}

	var statusFilter *models.JobStatus
	if req.Status != "" {
		s := models.JobStatus(req.Status)
		statusFilter = &s
	}

	jobs, err := s.jobStore.ListJobs(ctx, req.TenantId, statusFilter, limit, int(req.Offset))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list jobs")
	}

	var pbJobs []*pb.Job
	for _, j := range jobs {
		pbJobs = append(pbJobs, &pb.Job{
			Id:         j.ID,
			RequestId:  j.RequestID,
			TenantId:   j.TenantID,
			Status:     string(j.Status),
			Priority:   string(j.Priority),
			Payload:    j.Payload,
			MaxRetries: int32(j.MaxRetries),
			RetryCount: int32(j.RetryCount),
			LastError:  j.LastError,
		})
	}

	return &pb.ListJobsResponse{
		Jobs:    pbJobs,
		Total:   int32(len(pbJobs)),
		Message: "ok",
	}, nil
}

// ─────────────────────────────────────────
// Schedule RPC handlers (recurring jobs)
// ─────────────────────────────────────────

// CreateSchedule creates a new recurring job schedule
func (s *Scheduler) CreateSchedule(ctx context.Context, req *pb.CreateScheduleRequest) (*pb.CreateScheduleResponse, error) {
	// Validate inputs
	if req.TenantId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "tenant_id is required")
	}
	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "name is required")
	}
	if req.Payload == "" {
		return nil, status.Errorf(codes.InvalidArgument, "payload is required")
	}

	// Validate cron expression — rejects sub-minute intervals
	if err := ValidateCronExpr(req.CronExpr); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid cron expression: %v", err)
	}

	// Validate payload size
	if len(req.Payload) > s.cfg.MaxPayloadBytes {
		return nil, status.Errorf(codes.InvalidArgument, "payload exceeds maximum size of %d bytes", s.cfg.MaxPayloadBytes)
	}

	// Validate missed policy
	missedPolicy := models.MissedRunPolicy(req.MissedPolicy)
	if missedPolicy == "" {
		missedPolicy = models.MissedRunPolicySkip
	}
	switch missedPolicy {
	case models.MissedRunPolicySkip, models.MissedRunPolicyRunOnce, models.MissedRunPolicyRunAll:
		// valid
	default:
		return nil, status.Errorf(codes.InvalidArgument, "invalid missed_policy %q — must be SKIP, RUN_ONCE, or RUN_ALL", req.MissedPolicy)
	}

	priority := models.JobPriority(req.Priority)
	if priority == "" {
		priority = models.JobPriorityMedium
	}

	maxRetries := int(req.MaxRetries)
	if maxRetries == 0 {
		maxRetries = 3
	}

	runTimeout := time.Duration(req.RunTimeoutMs) * time.Millisecond
	if runTimeout == 0 {
		runTimeout = s.cfg.RunTimeout
	}

	// Compute first next_run_at
	ce := newCronEvaluator(s.scheduleStore, s.jobStore, s.log)
	nextRun, err := ce.nextRunTime(req.CronExpr, time.Now())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to compute next run time: %v", err)
	}

	sched := &models.Schedule{
		TenantID:     req.TenantId,
		Name:         req.Name,
		CronExpr:     req.CronExpr,
		Payload:      req.Payload,
		Priority:     priority,
		MaxRetries:   maxRetries,
		RunTimeout:   runTimeout,
		Enabled:      true,
		MissedPolicy: missedPolicy,
		NextRunAt:    nextRun,
	}

	created, err := s.scheduleStore.CreateSchedule(ctx, sched)
	if err != nil {
		s.log.Error("failed to create schedule", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to create schedule: %v", err)
	}

	s.log.Info("schedule created",
		zap.String("schedule_id", created.ID),
		zap.String("name", created.Name),
		zap.String("cron", created.CronExpr),
		zap.String("tenant_id", created.TenantID),
	)

	return &pb.CreateScheduleResponse{
		Success:    true,
		ScheduleId: created.ID,
		Message:    "schedule created",
		Schedule:   scheduleToProto(created),
	}, nil
}

// ListSchedules returns all schedules for a tenant
func (s *Scheduler) ListSchedules(ctx context.Context, req *pb.ListSchedulesRequest) (*pb.ListSchedulesResponse, error) {
	if req.TenantId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "tenant_id is required")
	}

	schedules, err := s.scheduleStore.ListSchedules(ctx, req.TenantId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list schedules")
	}

	var pbSchedules []*pb.Schedule
	for _, sched := range schedules {
		pbSchedules = append(pbSchedules, scheduleToProto(sched))
	}

	return &pb.ListSchedulesResponse{
		Schedules: pbSchedules,
		Message:   "ok",
	}, nil
}

// ToggleSchedule enables or disables a schedule
func (s *Scheduler) ToggleSchedule(ctx context.Context, req *pb.ToggleScheduleRequest) (*pb.ToggleScheduleResponse, error) {
	if req.ScheduleId == "" || req.TenantId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_id and tenant_id are required")
	}

	if err := s.scheduleStore.ToggleSchedule(ctx, req.ScheduleId, req.TenantId, req.Enabled); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to toggle schedule: %v", err)
	}

	action := "paused"
	if req.Enabled {
		action = "resumed"
	}

	s.log.Info("schedule toggled",
		zap.String("schedule_id", req.ScheduleId),
		zap.String("action", action),
	)

	return &pb.ToggleScheduleResponse{Success: true, Message: "schedule " + action}, nil
}

// DeleteSchedule removes a schedule
func (s *Scheduler) DeleteSchedule(ctx context.Context, req *pb.DeleteScheduleRequest) (*pb.DeleteScheduleResponse, error) {
	if req.ScheduleId == "" || req.TenantId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_id and tenant_id are required")
	}

	if err := s.scheduleStore.DeleteSchedule(ctx, req.ScheduleId, req.TenantId); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete schedule: %v", err)
	}

	s.log.Info("schedule deleted", zap.String("schedule_id", req.ScheduleId))

	return &pb.DeleteScheduleResponse{Success: true, Message: "schedule deleted"}, nil
}

// scheduleToProto converts a models.Schedule to a protobuf Schedule
func scheduleToProto(s *models.Schedule) *pb.Schedule {
	pbSched := &pb.Schedule{
		Id:           s.ID,
		TenantId:     s.TenantID,
		Name:         s.Name,
		CronExpr:     s.CronExpr,
		Payload:      s.Payload,
		Priority:     string(s.Priority),
		MaxRetries:   int32(s.MaxRetries),
		RunTimeoutMs: s.RunTimeout.Milliseconds(),
		Enabled:      s.Enabled,
		MissedPolicy: string(s.MissedPolicy),
		NextRunAtMs:  s.NextRunAt.UnixMilli(),
	}
	if s.LastRunAt != nil {
		pbSched.LastRunAtMs = s.LastRunAt.UnixMilli()
	}
	return pbSched
}

// ─────────────────────────────────────────
// Autopsy (dead letter forensics)
// ─────────────────────────────────────────

// generateAutopsy creates a forensic report for a dead-lettered job
func (s *Scheduler) generateAutopsy(jobID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	report, err := autopsy.GenerateAutopsy(ctx, s.db.Pool, jobID)
	if err != nil {
		s.log.Error("failed to generate autopsy", zap.String("job_id", jobID), zap.Error(err))
		return
	}

	if err := autopsy.StoreAutopsy(ctx, s.db.Pool, report); err != nil {
		s.log.Error("failed to store autopsy", zap.String("job_id", jobID), zap.Error(err))
		return
	}

	s.log.Info("autopsy generated",
		zap.String("job_id", jobID),
		zap.String("probable_cause", report.ProbableCause),
		zap.Int("attempts", report.TotalAttempts),
	)
}

// GetAutopsy returns the autopsy report for a job (tenant-scoped)
func (s *Scheduler) GetAutopsy(ctx context.Context, req *pb.GetAutopsyRequest) (*pb.GetAutopsyResponse, error) {
	if req.JobId == "" || req.TenantId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "job_id and tenant_id are required")
	}

	autopsyStore := db.NewAutopsyStore(s.db)
	stored, err := autopsyStore.GetAutopsyByJobID(ctx, req.JobId, req.TenantId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "autopsy not found: %v", err)
	}

	return &pb.GetAutopsyResponse{
		Found:     true,
		ReportJson: string(stored.Report),
		Message:   "ok",
	}, nil
}

// ListAutopsies returns recent autopsy reports for a tenant
func (s *Scheduler) ListAutopsies(ctx context.Context, req *pb.ListAutopsiesRequest) (*pb.ListAutopsiesResponse, error) {
	if req.TenantId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "tenant_id is required")
	}

	autopsyStore := db.NewAutopsyStore(s.db)
	autopsies, err := autopsyStore.ListAutopsies(ctx, req.TenantId, int(req.Limit))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list autopsies: %v", err)
	}

	var summaries []*pb.AutopsySummary
	for _, a := range autopsies {
		summaries = append(summaries, &pb.AutopsySummary{
			Id:        a.ID,
			JobId:     a.JobID,
			TenantId:  a.TenantID,
			CreatedAt: a.CreatedAt.Format(time.RFC3339),
		})
	}

	return &pb.ListAutopsiesResponse{
		Autopsies: summaries,
		Message:   "ok",
	}, nil
}

// ─────────────────────────────────────────
// Background loops
// ─────────────────────────────────────────

// startHeartbeatMonitor runs in the background and reaps dead workers
func (s *Scheduler) startHeartbeatMonitor(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reapDeadWorkers(ctx)
		}
	}
}

// reapDeadWorkers finds workers that missed heartbeats and marks them dead
func (s *Scheduler) reapDeadWorkers(ctx context.Context) {
	stale, err := s.workerStore.GetStaleWorkers(ctx, s.cfg.HeartbeatTimeout)
	if err != nil {
		s.log.Error("failed to get stale workers", zap.Error(err))
		return
	}

	for _, worker := range stale {
		s.log.Warn("worker missed heartbeat — marking dead",
			zap.String("worker_id", worker.ID),
			zap.Time("last_heartbeat", worker.LastHeartbeat),
		)

		if err := s.workerStore.MarkWorkerDead(ctx, worker.ID); err != nil {
			s.log.Error("failed to mark worker dead", zap.Error(err))
			continue
		}
		workersReaped.Inc()
	}
}

// startReconciler runs in the background and reclaims expired leases
func (s *Scheduler) startReconciler(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count, err := s.jobStore.ReclaimExpiredJobs(ctx)
			if err != nil {
				s.log.Error("reconciler failed", zap.Error(err))
				continue
			}
			if count > 0 {
				jobsReclaimed.Add(float64(count))
				s.log.Info("reconciler reclaimed jobs",
					zap.Int("count", count),
				)
			}
		}
	}
}

// startDispatcher runs in the background and dispatches pending jobs to workers
func (s *Scheduler) startDispatcher(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.dispatchPendingJobs(ctx)
		}
	}
}

// dispatchPendingJobs claims and sends pending jobs to available workers
func (s *Scheduler) dispatchPendingJobs(ctx context.Context) {
	for {
		// Get least loaded worker
		worker, err := s.workerStore.GetLeastLoadedWorker(ctx)
		if err != nil || worker == nil {
			return // no available workers
		}

		// Atomically claim next job
		job, err := s.jobStore.ClaimNextJob(ctx, worker.ID, s.cfg.LeaseDuration)
		if err != nil || job == nil {
			return // no pending jobs
		}

		// Track dispatch latency from creation to now
		if job.CreatedAt.IsZero() == false {
			dispatchLatency.Observe(time.Since(job.CreatedAt).Seconds())
		}
		jobsDispatched.Inc()

		// Connect to worker and dispatch
		go s.dispatchToWorker(ctx, job, worker)
	}
}

// dispatchToWorker sends a job to a specific worker via gRPC
func (s *Scheduler) dispatchToWorker(ctx context.Context, job *models.Job, worker *models.Worker) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("panic in dispatchToWorker", zap.Any("panic", r))
		}
	}()

	s.log.Info("dispatching job to worker",
		zap.String("job_id", job.ID),
		zap.String("worker_id", worker.ID),
		zap.String("address", worker.Address),
	)

	var transportCreds grpc.DialOption
	if s.cfg.TLSEnabled {
		tlsCreds, err := credentials.NewClientTLSFromFile(s.cfg.TLSCertFile, "")
		if err != nil {
			s.log.Error("failed to load TLS credentials for worker connection", zap.Error(err))
			return
		}
		transportCreds = grpc.WithTransportCredentials(tlsCreds)
	} else {
		transportCreds = grpc.WithTransportCredentials(insecure.NewCredentials())
	}
	conn, err := grpc.NewClient("dns:///"+worker.Address, transportCreds)
	if err != nil {
		s.log.Error("failed to connect to worker",
			zap.String("worker_id", worker.ID),
			zap.String("address", worker.Address),
			zap.Error(err),
		)
		return
	}
	defer conn.Close()

	client := pb.NewWorkerServiceClient(conn)
	ctx, cancel := context.WithTimeout(ctx, s.cfg.DispatchTimeout)
	defer cancel()

	// Attach worker token so the worker can authenticate us
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+s.cfg.WorkerTokenSecret)

	resp, err := client.DispatchJob(ctx, &pb.DispatchRequest{
		Job: &pb.Job{
			Id:           job.ID,
			RequestId:    job.RequestID,
			TenantId:     job.TenantID,
			Status:       string(job.Status),
			Priority:     string(job.Priority),
			Payload:      job.Payload,
			MaxRetries:   int32(job.MaxRetries),
			RetryCount:   int32(job.RetryCount),
			LeaseVersion: int32(job.LeaseVersion),
			RunTimeoutMs: job.RunTimeout.Milliseconds(),
		},
		LeaseId:      job.LeaseID,
		LeaseVersion: int32(job.LeaseVersion),
	})

	if err != nil {
		s.log.Error("failed to dispatch job to worker",
			zap.String("job_id", job.ID),
			zap.String("worker_id", worker.ID),
			zap.String("address", worker.Address),
			zap.Error(err),
		)
		return
	}
	if !resp.Accepted {
		s.log.Warn("worker rejected job dispatch",
			zap.String("job_id", job.ID),
			zap.String("worker_id", worker.ID),
			zap.String("message", resp.Message),
		)
	}
}

// startMetricsUpdater periodically refreshes gauge metrics from the database
func (s *Scheduler) startMetricsUpdater(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.updateGaugeMetrics(ctx)
		}
	}
}

func (s *Scheduler) updateGaugeMetrics(ctx context.Context) {
	// Job queue depth by status
	statuses := []string{"PENDING", "DISPATCHED", "RUNNING", "SUCCEEDED", "FAILED", "DEAD_LETTER"}
	for _, st := range statuses {
		var count int
		err := s.db.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM jobs WHERE status = $1", st).Scan(&count)
		if err == nil {
			jobQueueDepth.WithLabelValues(st).Set(float64(count))
		}
	}

	// Active worker count
	workers, err := s.workerStore.GetActiveWorkers(ctx)
	if err == nil {
		workersActive.Set(float64(len(workers)))
	}
}

// getTenantJobCount returns how many active jobs a tenant has
func (s *Scheduler) getTenantJobCount(ctx context.Context, tenantID string) (int, error) {
	query := `
		SELECT COUNT(*) FROM jobs
		WHERE tenant_id = $1
		  AND status NOT IN ('SUCCEEDED', 'CANCELLED', 'DEAD_LETTER')`

	var count int
	err := s.db.Pool.QueryRow(ctx, query, tenantID).Scan(&count)
	return count, err
}

// ─────────────────────────────────────────
// Main entrypoint
// ─────────────────────────────────────────

func main() {
	log, _ := zap.NewProduction()
	defer log.Sync()

	cfg := config.Default()

	// Required environment variables — no hardcoded secrets
	cfg.DatabaseURL = os.Getenv("DATABASE_URL")
	if cfg.DatabaseURL == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}
	cfg.APIToken = os.Getenv("API_TOKEN")
	if cfg.APIToken == "" {
		log.Fatal("API_TOKEN environment variable is required")
	}
	cfg.WorkerTokenSecret = os.Getenv("WORKER_TOKEN")
	if cfg.WorkerTokenSecret == "" {
		log.Fatal("WORKER_TOKEN environment variable is required")
	}

	// TLS configuration (optional)
	if os.Getenv("TLS_ENABLED") == "true" {
		cfg.TLSEnabled = true
		cfg.TLSCertFile = os.Getenv("TLS_CERT_FILE")
		cfg.TLSKeyFile = os.Getenv("TLS_KEY_FILE")
		if cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" {
			log.Fatal("TLS_CERT_FILE and TLS_KEY_FILE are required when TLS_ENABLED=true")
		}
	}

	ctx := context.Background()
	database, err := db.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal("failed to connect to database", zap.Error(err))
	}
	defer database.Close()

	log.Info("connected to database")

	scheduler := NewScheduler(cfg, database, log)

	// ── Leader election ──
	// Background loops (dispatch, heartbeat monitor, reconciler, cron) only run
	// on the leader. All instances serve gRPC RPCs (stateless DB reads/writes).
	var leaderLoopCtx context.Context
	var leaderLoopCancel context.CancelFunc

	onPromote := func() {
		// Reclaim expired jobs on promotion (same as old startup reconciliation)
		reclaimCtx, reclaimCancel := context.WithTimeout(ctx, 10*time.Second)
		defer reclaimCancel()
		count, err := scheduler.jobStore.ReclaimExpiredJobs(reclaimCtx)
		if err != nil {
			log.Error("promotion reconciliation failed", zap.Error(err))
		} else if count > 0 {
			log.Info("promotion reconciliation complete", zap.Int("reclaimed", count))
		}

		leaderLoopCtx, leaderLoopCancel = context.WithCancel(ctx)

		go scheduler.startHeartbeatMonitor(leaderLoopCtx)
		go scheduler.startReconciler(leaderLoopCtx)
		go scheduler.startDispatcher(leaderLoopCtx)
		go scheduler.startMetricsUpdater(leaderLoopCtx)

		cronEval := newCronEvaluator(scheduler.scheduleStore, scheduler.jobStore, log)
		go cronEval.run(leaderLoopCtx)

		log.Info("leader background loops started")
	}

	onDemote := func() {
		if leaderLoopCancel != nil {
			leaderLoopCancel()
			leaderLoopCancel = nil
		}
		log.Info("leader background loops stopped — operating as standby")
	}

	elector := NewLeaderElector(database.Pool, log, onPromote, onDemote)
	go elector.Run(ctx)

	// ── HTTP server (metrics + health) ──
	// Health endpoints run on ALL instances (leader and standby)
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())

		// /health — basic liveness: can we reach the database?
		// Does NOT expose internal error details to avoid information leakage.
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			if err := database.HealthCheck(r.Context()); err != nil {
				log.Warn("health check failed", zap.Error(err))
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprint(w, `{"status":"unhealthy"}`)
				return
			}
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"status":"healthy"}`)
		})

		// /ready — readiness: are we connected to the DB and able to serve RPCs?
		// Both leader and standby are "ready" to serve gRPC RPCs.
		// The response includes leader status for observability.
		mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
			if err := database.HealthCheck(r.Context()); err != nil {
				log.Warn("readiness check failed", zap.Error(err))
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprint(w, `{"ready":false,"leader":false}`)
				return
			}
			isLeader := elector.IsLeader()
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"ready":true,"leader":%v}`, isLeader)
		})

		metricsAddr := fmt.Sprintf(":%d", cfg.MetricsPort)
		log.Info("HTTP server starting (metrics + health)", zap.String("address", metricsAddr))
		if err := http.ListenAndServe(metricsAddr, mux); err != nil {
			log.Error("HTTP server failed", zap.Error(err))
		}
	}()

	// ── gRPC server ──
	// All instances serve RPCs — they're stateless DB reads/writes.
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.SchedulerPort))
	if err != nil {
		log.Fatal("failed to listen", zap.Error(err))
	}

	rl := newRateLimiter(cfg.RateLimitPerSecond, cfg.RateLimitBurst, cfg.RateLimitWindow)

	serverOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			authInterceptor(cfg.APIToken, cfg.WorkerTokenSecret),
			rateLimitInterceptor(rl),
		),
	}

	if cfg.TLSEnabled {
		creds, err := credentials.NewServerTLSFromFile(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			log.Fatal("failed to load TLS credentials", zap.Error(err))
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
		log.Info("TLS enabled for gRPC server")
	}

	grpcServer := grpc.NewServer(serverOpts...)
	pb.RegisterSchedulerServiceServer(grpcServer, scheduler)

	log.Info("scheduler starting",
		zap.Int("port", cfg.SchedulerPort),
		zap.String("mode", "HA — competing for leader lock"),
	)

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit
		log.Info("shutting down scheduler...")
		if leaderLoopCancel != nil {
			leaderLoopCancel()
		}
		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal("gRPC server failed", zap.Error(err))
	}
}
