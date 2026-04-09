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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Kulnoorbajwa/distributed-scheduler/config"
	"github.com/Kulnoorbajwa/distributed-scheduler/internal/db"
	"github.com/Kulnoorbajwa/distributed-scheduler/internal/models"
	pb "github.com/Kulnoorbajwa/distributed-scheduler/proto"
)

// Scheduler is the master node — it holds all the state
// and coordinates workers
type Scheduler struct {
	pb.UnimplementedSchedulerServiceServer
	cfg         *config.Config
	db          *db.DB
	jobStore    *db.JobStore
	workerStore *db.WorkerStore
	log         *zap.Logger
}

// NewScheduler creates a new scheduler instance
func NewScheduler(cfg *config.Config, database *db.DB, log *zap.Logger) *Scheduler {
	return &Scheduler{
		cfg:         cfg,
		db:          database,
		jobStore:    db.NewJobStore(database),
		workerStore: db.NewWorkerStore(database),
		log:         log,
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
		err = s.jobStore.FailJob(ctx, req.JobId, req.WorkerId, req.ErrorMessage, int(req.LeaseVersion))
		if err == nil {
			jobsCompleted.WithLabelValues("failed").Inc()
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

	conn, err := grpc.NewClient("dns:///"+worker.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

	ctx := context.Background()
	database, err := db.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal("failed to connect to database", zap.Error(err))
	}
	defer database.Close()

	log.Info("connected to database")

	jobStore := db.NewJobStore(database)
	count, err := jobStore.ReclaimExpiredJobs(ctx)
	if err != nil {
		log.Error("startup reconciliation failed", zap.Error(err))
	} else {
		log.Info("startup reconciliation complete", zap.Int("reclaimed", count))
	}

	scheduler := NewScheduler(cfg, database, log)

	bgCtx, bgCancel := context.WithCancel(ctx)
	defer bgCancel()

	go scheduler.startHeartbeatMonitor(bgCtx)
	go scheduler.startReconciler(bgCtx)
	go scheduler.startDispatcher(bgCtx)
	go scheduler.startMetricsUpdater(bgCtx)

	// Prometheus metrics HTTP server
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		metricsAddr := fmt.Sprintf(":%d", cfg.MetricsPort)
		log.Info("metrics server starting", zap.String("address", metricsAddr))
		if err := http.ListenAndServe(metricsAddr, mux); err != nil {
			log.Error("metrics server failed", zap.Error(err))
		}
	}()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.SchedulerPort))
	if err != nil {
		log.Fatal("failed to listen", zap.Error(err))
	}

	rl := newRateLimiter(cfg.RateLimitPerSecond, cfg.RateLimitBurst, cfg.RateLimitWindow)

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			authInterceptor(cfg.APIToken, cfg.WorkerTokenSecret),
			rateLimitInterceptor(rl),
		),
	)
	pb.RegisterSchedulerServiceServer(grpcServer, scheduler)

	log.Info("scheduler starting", zap.Int("port", cfg.SchedulerPort))

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit
		log.Info("shutting down scheduler...")
		bgCancel()
		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal("gRPC server failed", zap.Error(err))
	}
}
