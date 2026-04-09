package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Kulnoorbajwa/distributed-scheduler/config"
	pb "github.com/Kulnoorbajwa/distributed-scheduler/proto"
)

// Worker is a node that executes jobs assigned by the scheduler
type Worker struct {
	pb.UnimplementedWorkerServiceServer
	id            string
	tenantID      string
	address       string
	schedulerAddr string
	workerToken   string
	cfg           *config.Config
	log           *zap.Logger
	activeJobs    map[string]*runningJob // jobID → running job
	cancelFuncs   map[string]context.CancelFunc
}

// runningJob tracks a job currently being executed
type runningJob struct {
	jobID        string
	leaseVersion int32
	startedAt    time.Time
}

// NewWorker creates a new worker instance
func NewWorker(id, tenantID, address, schedulerAddr, workerToken string, cfg *config.Config, log *zap.Logger) *Worker {
	return &Worker{
		id:            id,
		tenantID:      tenantID,
		address:       address,
		schedulerAddr: schedulerAddr,
		workerToken:   workerToken,
		cfg:           cfg,
		log:           log,
		activeJobs:    make(map[string]*runningJob),
		cancelFuncs:   make(map[string]context.CancelFunc),
	}
}

// authCtx attaches the worker auth token to an outgoing gRPC context
func (w *Worker) authCtx(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+w.workerToken)
}

// workerAuthInterceptor validates that incoming RPCs (from the scheduler) carry the correct token.
func workerAuthInterceptor(workerToken string) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Errorf(codes.Unauthenticated, "missing metadata")
		}

		tokens := md.Get("authorization")
		if len(tokens) == 0 {
			return nil, status.Errorf(codes.Unauthenticated, "missing authorization token")
		}

		token := tokens[0]
		if len(token) > 7 && token[:7] == "Bearer " {
			token = token[7:]
		}

		if token != workerToken {
			return nil, status.Errorf(codes.PermissionDenied, "invalid token")
		}

		return handler(ctx, req)
	}
}

// ─────────────────────────────────────────
// Scheduler-facing RPC handlers
// ─────────────────────────────────────────

// DispatchJob is called BY the scheduler to assign a job to this worker
func (w *Worker) DispatchJob(ctx context.Context, req *pb.DispatchRequest) (*pb.DispatchResponse, error) {
	job := req.Job

	w.log.Info("job dispatched to worker",
		zap.String("job_id", job.Id),
		zap.String("priority", job.Priority),
		zap.Int("lease_version", int(req.LeaseVersion)),
	)

	// Check capacity
	if len(w.activeJobs) >= w.cfg.MaxWorkerCount {
		return &pb.DispatchResponse{
			Accepted: false,
			Message:  "worker at capacity",
		}, nil
	}

	// Track the job
	jobCtx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(job.RunTimeoutMs)*time.Millisecond,
	)

	w.activeJobs[job.Id] = &runningJob{
		jobID:        job.Id,
		leaseVersion: req.LeaseVersion,
		startedAt:    time.Now(),
	}
	w.cancelFuncs[job.Id] = cancel

	// Execute job in background
	go w.executeJob(jobCtx, job, req.LeaseVersion)

	return &pb.DispatchResponse{Accepted: true, Message: "accepted"}, nil
}

// CancelJob is called BY the scheduler to cancel a running job
func (w *Worker) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
	cancel, exists := w.cancelFuncs[req.JobId]
	if !exists {
		return &pb.CancelJobResponse{
			Success: false,
			Message: "job not found on this worker",
		}, nil
	}

	// Cancel the job's context — this stops execution
	cancel()
	delete(w.activeJobs, req.JobId)
	delete(w.cancelFuncs, req.JobId)

	w.log.Info("job cancelled by scheduler", zap.String("job_id", req.JobId))

	return &pb.CancelJobResponse{Success: true, Message: "cancelled"}, nil
}

// ─────────────────────────────────────────
// Job execution
// ─────────────────────────────────────────

// executeJob runs a job and reports the result back to the scheduler
func (w *Worker) executeJob(ctx context.Context, job *pb.Job, leaseVersion int32) {
	defer func() {
		delete(w.activeJobs, job.Id)
		delete(w.cancelFuncs, job.Id)
	}()

	// Start lease renewal in background
	renewCtx, cancelRenew := context.WithCancel(ctx)
	defer cancelRenew()
	go w.renewLease(renewCtx, job.Id, leaseVersion)

	w.log.Info("executing job",
		zap.String("job_id", job.Id),
		zap.String("payload", job.Payload),
	)

	// Execute the actual job payload
	err := w.runJob(ctx, job)

	// Report result back to scheduler
	w.reportResult(job.Id, leaseVersion, err)
}

// runJob parses the job payload and executes real work
func (w *Worker) runJob(ctx context.Context, job *pb.Job) error {
	return w.executePayload(ctx, job.Payload)
}

// ─────────────────────────────────────────
// Scheduler communication
// ─────────────────────────────────────────

// schedulerClient creates a gRPC connection to the scheduler
func (w *Worker) schedulerClient() (pb.SchedulerServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient("dns:///"+w.schedulerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to scheduler: %w", err)
	}
	return pb.NewSchedulerServiceClient(conn), conn, nil
}

// register sends this worker's info to the scheduler
func (w *Worker) register(ctx context.Context) error {
	client, conn, err := w.schedulerClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	resp, err := client.RegisterWorker(w.authCtx(ctx), &pb.RegisterRequest{
		Worker: &pb.Worker{
			Id:       w.id,
			TenantId: w.tenantID,
			Address:  w.address,
			MaxJobs:  10,
			CpuSlots: 4,
			MemoryMb: 512,
			Version:  "1.0.0",
		},
	})

	if err != nil || !resp.Success {
		return fmt.Errorf("registration failed: %v", err)
	}

	w.log.Info("registered with scheduler",
		zap.String("worker_id", w.id),
		zap.String("scheduler", w.schedulerAddr),
	)
	return nil
}

// startHeartbeat sends heartbeats to the scheduler on an interval
func (w *Worker) startHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.sendHeartbeat(ctx)
		}
	}
}

// sendHeartbeat pings the scheduler with current status
func (w *Worker) sendHeartbeat(ctx context.Context) {
	client, conn, err := w.schedulerClient()
	if err != nil {
		w.log.Error("heartbeat failed — cannot reach scheduler", zap.Error(err))
		return
	}
	defer conn.Close()

	hbCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	resp, err := client.Heartbeat(w.authCtx(hbCtx), &pb.HeartbeatRequest{
		WorkerId:   w.id,
		ActiveJobs: int32(len(w.activeJobs)),
		Status:     "ACTIVE",
	})

	if err != nil {
		w.log.Error("heartbeat error", zap.Error(err))
		return
	}

	// Scheduler told us to re-register — we were marked dead
	if !resp.Success && resp.Message == "RE_REGISTER" {
		w.log.Warn("scheduler marked us dead — re-registering")
		if err := w.register(ctx); err != nil {
			w.log.Error("re-registration failed", zap.Error(err))
		}
	}
}

// renewLease keeps a job's lease alive while it's running
func (w *Worker) renewLease(ctx context.Context, jobID string, leaseVersion int32) {
	ticker := time.NewTicker(w.cfg.LeaseRenewAt)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			client, conn, err := w.schedulerClient()
			if err != nil {
				w.log.Error("lease renewal failed", zap.Error(err))
				continue
			}

			renewCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			resp, err := client.RenewLease(w.authCtx(renewCtx), &pb.LeaseRenewRequest{
				JobId:        jobID,
				WorkerId:     w.id,
				LeaseVersion: leaseVersion,
			})
			cancel()
			conn.Close()

			if err != nil || !resp.Success {
				w.log.Warn("lease renewal rejected — job may have been reassigned",
					zap.String("job_id", jobID),
				)
				return
			}
		}
	}
}

// reportRunning tells the scheduler a job has started executing
func (w *Worker) reportRunning(jobID string, leaseVersion int32) {
	client, conn, err := w.schedulerClient()
	if err != nil {
		w.log.Error("failed to report running", zap.Error(err))
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = client.ReportResult(w.authCtx(ctx), &pb.JobResultRequest{
		JobId:        jobID,
		WorkerId:     w.id,
		LeaseVersion: leaseVersion,
		Success:      true,
	})
	if err != nil {
		w.log.Error("failed to report running state", zap.Error(err))
	}
}

// reportResult sends the final job outcome to the scheduler
func (w *Worker) reportResult(jobID string, leaseVersion int32, jobErr error) {
	client, conn, err := w.schedulerClient()
	if err != nil {
		w.log.Error("failed to connect to scheduler for result", zap.Error(err))
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req := &pb.JobResultRequest{
		JobId:        jobID,
		WorkerId:     w.id,
		LeaseVersion: leaseVersion,
		Success:      jobErr == nil,
	}
	if jobErr != nil {
		req.ErrorMessage = jobErr.Error()
	}

	resp, err := client.ReportResult(w.authCtx(ctx), req)
	if err != nil {
		w.log.Error("failed to report result", zap.Error(err))
		return
	}

	if !resp.Accepted {
		w.log.Warn("result rejected by scheduler — stale lease",
			zap.String("job_id", jobID),
		)
		return
	}

	w.log.Info("job result accepted",
		zap.String("job_id", jobID),
		zap.Bool("success", jobErr == nil),
	)
}

// ─────────────────────────────────────────
// Main entrypoint
// ─────────────────────────────────────────

func main() {
	log, _ := zap.NewProduction()
	defer log.Sync()

	cfg := config.Default()

	// Worker identity from environment variables
	// WORKER_ID must be a valid UUID to match the PostgreSQL schema
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = uuid.New().String()
	} else if _, err := uuid.Parse(workerID); err != nil {
		log.Fatal("WORKER_ID must be a valid UUID", zap.String("worker_id", workerID), zap.Error(err))
	}

	tenantID := os.Getenv("TENANT_ID")
	if tenantID == "" {
		tenantID = "default"
	}

	workerAddr := os.Getenv("WORKER_ADDR")
	if workerAddr == "" {
		workerAddr = fmt.Sprintf("localhost:%d", cfg.WorkerPort)
	}

	schedulerAddr := os.Getenv("SCHEDULER_ADDR")
	if schedulerAddr == "" {
		schedulerAddr = fmt.Sprintf("localhost:%d", cfg.SchedulerPort)
	}

	workerToken := os.Getenv("WORKER_TOKEN")
	if workerToken == "" {
		log.Fatal("WORKER_TOKEN environment variable is required")
	}

	worker := NewWorker(workerID, tenantID, workerAddr, schedulerAddr, workerToken, cfg, log)

	ctx := context.Background()

	// Register with scheduler
	if err := worker.register(ctx); err != nil {
		log.Fatal("failed to register with scheduler", zap.Error(err))
	}

	// Start heartbeat loop
	bgCtx, bgCancel := context.WithCancel(ctx)
	defer bgCancel()
	go worker.startHeartbeat(bgCtx)

	// Start gRPC server so scheduler can dispatch jobs to us
	lis, err := net.Listen("tcp", workerAddr)
	if err != nil {
		log.Fatal("failed to listen", zap.Error(err))
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(workerAuthInterceptor(workerToken)),
	)
	pb.RegisterWorkerServiceServer(grpcServer, worker)

	log.Info("worker starting",
		zap.String("worker_id", workerID),
		zap.String("address", workerAddr),
		zap.String("scheduler", schedulerAddr),
	)

	// Graceful shutdown
	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit
		log.Info("shutting down worker...")
		bgCancel()
		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal("gRPC server failed", zap.Error(err))
	}
}