package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "github.com/Kulnoorbajwa/distributed-scheduler/proto"
)

func usage() {
	fmt.Println(`Usage: client <command> [options]

Commands:
  submit            Submit a new job
  status            Check job status
  list              List jobs
  cancel            Cancel a job
  autopsy           View autopsy report for a dead-lettered job
  autopsy-list      List recent autopsy reports
  schedule-create   Create a recurring schedule
  schedule-list     List schedules
  schedule-pause    Pause a schedule
  schedule-resume   Resume a schedule
  schedule-delete   Delete a schedule
  demo              Run a full demo (submit multiple jobs, poll status)

Submit options:
  --payload  JSON payload (required)
  --priority HIGH|MEDIUM|LOW (default: MEDIUM)
  --retries  Max retries (default: 3)
  --timeout  Run timeout in ms (default: 60000)
  --tenant   Tenant ID (default: "default")

Status options:
  --job-id   Job ID to check (required)

List options:
  --tenant   Tenant ID (default: "default")
  --status   Filter by status (optional)

Cancel options:
  --job-id   Job ID to cancel (required)
  --tenant   Tenant ID (default: "default")

Examples:
  client submit --payload '{"type":"shell","command":"echo hello world"}'
  client submit --payload '{"type":"http","method":"GET","url":"https://httpbin.org/get"}' --priority HIGH
  client submit --payload '{"type":"sleep","duration_ms":5000}'
  client submit --payload '{"type":"fail","message":"test retry logic"}' --retries 3
  client status --job-id <uuid>
  client list --tenant default
  client cancel --job-id <uuid>
  client schedule-create --name "hourly" --cron "0 * * * *" --payload '{"type":"shell","command":"echo tick"}'
  client schedule-list --tenant default
  client schedule-pause --id <uuid> --tenant default
  client schedule-resume --id <uuid> --tenant default
  client schedule-delete --id <uuid> --tenant default
  client demo`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	schedulerAddr := os.Getenv("SCHEDULER_ADDR")
	if schedulerAddr == "" {
		schedulerAddr = "localhost:50051"
	}

	apiToken := os.Getenv("API_TOKEN")
	if apiToken == "" {
		fmt.Fprintln(os.Stderr, "Error: API_TOKEN environment variable is required")
		os.Exit(1)
	}

	var transportCreds grpc.DialOption
	if os.Getenv("TLS_ENABLED") == "true" {
		certFile := os.Getenv("TLS_CERT_FILE")
		if certFile == "" {
			fmt.Fprintln(os.Stderr, "Error: TLS_CERT_FILE is required when TLS_ENABLED=true")
			os.Exit(1)
		}
		creds, err := credentials.NewClientTLSFromFile(certFile, "")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to load TLS credentials: %v\n", err)
			os.Exit(1)
		}
		transportCreds = grpc.WithTransportCredentials(creds)
	} else {
		transportCreds = grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	conn, err := grpc.NewClient("dns:///"+schedulerAddr, transportCreds)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to scheduler at %s: %v\n", schedulerAddr, err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewSchedulerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Attach API token to all outgoing RPCs
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+apiToken)

	switch os.Args[1] {
	case "submit":
		cmdSubmit(ctx, client, os.Args[2:])
	case "status":
		cmdStatus(ctx, client, os.Args[2:])
	case "list":
		cmdList(ctx, client, os.Args[2:])
	case "cancel":
		cmdCancel(ctx, client, os.Args[2:])
	case "autopsy":
		cmdAutopsy(ctx, client, os.Args[2:])
	case "autopsy-list":
		cmdAutopsyList(ctx, client, os.Args[2:])
	case "schedule-create":
		cmdScheduleCreate(ctx, client, os.Args[2:])
	case "schedule-list":
		cmdScheduleList(ctx, client, os.Args[2:])
	case "schedule-pause":
		cmdScheduleToggle(ctx, client, os.Args[2:], false)
	case "schedule-resume":
		cmdScheduleToggle(ctx, client, os.Args[2:], true)
	case "schedule-delete":
		cmdScheduleDelete(ctx, client, os.Args[2:])
	case "demo":
		cmdDemo(ctx, client)
	default:
		usage()
		os.Exit(1)
	}
}

func parseFlag(args []string, flag string, def string) string {
	for i, arg := range args {
		if arg == flag && i+1 < len(args) {
			return args[i+1]
		}
	}
	return def
}

func cmdSubmit(ctx context.Context, client pb.SchedulerServiceClient, args []string) {
	payload := parseFlag(args, "--payload", "")
	if payload == "" {
		fmt.Fprintln(os.Stderr, "Error: --payload is required")
		os.Exit(1)
	}

	priority := parseFlag(args, "--priority", "MEDIUM")
	retries := parseFlag(args, "--retries", "3")
	timeout := parseFlag(args, "--timeout", "60000")
	tenant := parseFlag(args, "--tenant", "default")

	var maxRetries int32
	fmt.Sscanf(retries, "%d", &maxRetries)
	var timeoutMs int64
	fmt.Sscanf(timeout, "%d", &timeoutMs)

	resp, err := client.SubmitJob(ctx, &pb.SubmitJobRequest{
		RequestId:    uuid.New().String(),
		TenantId:     tenant,
		Priority:     priority,
		Payload:      payload,
		MaxRetries:   maxRetries,
		RunTimeoutMs: timeoutMs,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Submit failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Job submitted successfully\n")
	fmt.Printf("  Job ID:   %s\n", resp.JobId)
	fmt.Printf("  Priority: %s\n", priority)
	fmt.Printf("  Payload:  %s\n", payload)
}

func cmdStatus(ctx context.Context, client pb.SchedulerServiceClient, args []string) {
	jobID := parseFlag(args, "--job-id", "")
	if jobID == "" {
		fmt.Fprintln(os.Stderr, "Error: --job-id is required")
		os.Exit(1)
	}

	resp, err := client.GetJob(ctx, &pb.GetJobRequest{JobId: jobID})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Status check failed: %v\n", err)
		os.Exit(1)
	}
	if !resp.Found {
		fmt.Println("Job not found")
		os.Exit(1)
	}

	job := resp.Job
	fmt.Printf("Job %s\n", job.Id)
	fmt.Printf("  Status:       %s\n", job.Status)
	fmt.Printf("  Priority:     %s\n", job.Priority)
	fmt.Printf("  Payload:      %s\n", job.Payload)
	fmt.Printf("  Retries:      %d / %d\n", job.RetryCount, job.MaxRetries)
	if job.LastError != "" {
		fmt.Printf("  Last Error:   %s\n", job.LastError)
	}
}

func cmdList(ctx context.Context, client pb.SchedulerServiceClient, args []string) {
	tenant := parseFlag(args, "--tenant", "default")
	statusFilter := parseFlag(args, "--status", "")

	resp, err := client.ListJobs(ctx, &pb.ListJobsRequest{
		TenantId: tenant,
		Status:   statusFilter,
		Limit:    50,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "List failed: %v\n", err)
		os.Exit(1)
	}

	if len(resp.Jobs) == 0 {
		fmt.Println("No jobs found")
		return
	}

	fmt.Printf("%-38s %-12s %-8s %-4s %s\n", "JOB ID", "STATUS", "PRIORITY", "RETRY", "PAYLOAD")
	fmt.Println(strings.Repeat("-", 100))
	for _, job := range resp.Jobs {
		payload := job.Payload
		if len(payload) > 40 {
			payload = payload[:40] + "..."
		}
		fmt.Printf("%-38s %-12s %-8s %d/%-3d %s\n",
			job.Id, job.Status, job.Priority, job.RetryCount, job.MaxRetries, payload)
	}
	fmt.Printf("\nTotal: %d jobs\n", resp.Total)
}

func cmdCancel(ctx context.Context, client pb.SchedulerServiceClient, args []string) {
	jobID := parseFlag(args, "--job-id", "")
	tenant := parseFlag(args, "--tenant", "default")
	if jobID == "" {
		fmt.Fprintln(os.Stderr, "Error: --job-id is required")
		os.Exit(1)
	}

	resp, err := client.CancelJob(ctx, &pb.CancelJobClientRequest{
		JobId:    jobID,
		TenantId: tenant,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cancel failed: %v\n", err)
		os.Exit(1)
	}

	if resp.Success {
		fmt.Printf("Job %s cancelled\n", jobID)
	} else {
		fmt.Printf("Cancel failed: %s\n", resp.Message)
	}
}

func cmdDemo(ctx context.Context, client pb.SchedulerServiceClient) {
	fmt.Println("=== Distributed Scheduler Demo ===")
	fmt.Println()

	// Submit a variety of jobs
	jobs := []struct {
		name     string
		priority string
		payload  map[string]interface{}
	}{
		{"echo test", "HIGH", map[string]interface{}{"type": "shell", "command": "echo Hello from the distributed scheduler!"}},
		{"date command", "HIGH", map[string]interface{}{"type": "shell", "command": "date"}},
		{"list files", "MEDIUM", map[string]interface{}{"type": "shell", "command": "ls -la /"}},
		{"hostname check", "MEDIUM", map[string]interface{}{"type": "shell", "command": "hostname"}},
		{"sleep job (3s)", "LOW", map[string]interface{}{"type": "sleep", "duration_ms": 3000}},
		{"sleep job (5s)", "LOW", map[string]interface{}{"type": "sleep", "duration_ms": 5000}},
		{"deliberate failure", "MEDIUM", map[string]interface{}{"type": "fail", "message": "testing retry mechanism"}},
	}

	var jobIDs []string

	for _, j := range jobs {
		payloadJSON, _ := json.Marshal(j.payload)

		resp, err := client.SubmitJob(ctx, &pb.SubmitJobRequest{
			RequestId:    uuid.New().String(),
			TenantId:     "default",
			Priority:     j.priority,
			Payload:      string(payloadJSON),
			MaxRetries:   3,
			RunTimeoutMs: 60000,
		})
		if err != nil {
			fmt.Printf("  FAIL  %s: %v\n", j.name, err)
			continue
		}

		fmt.Printf("  [%s] %-20s  job_id=%s\n", j.priority, j.name, resp.JobId)
		jobIDs = append(jobIDs, resp.JobId)
	}

	fmt.Printf("\nSubmitted %d jobs. Polling status...\n\n", len(jobIDs))

	// Poll until all jobs are terminal
	for round := 1; round <= 20; round++ {
		time.Sleep(3 * time.Second)

		pending, running, done := 0, 0, 0
		for _, id := range jobIDs {
			resp, err := client.GetJob(ctx, &pb.GetJobRequest{JobId: id})
			if err != nil || !resp.Found {
				continue
			}
			switch resp.Job.Status {
			case "PENDING", "DISPATCHED":
				pending++
			case "RUNNING":
				running++
			default:
				done++
			}
		}

		fmt.Printf("  [%2ds] pending=%d  running=%d  completed=%d / %d\n",
			round*3, pending, running, done, len(jobIDs))

		if done == len(jobIDs) {
			break
		}
	}

	// Final summary
	fmt.Println("\n=== Final Results ===")
	for _, id := range jobIDs {
		resp, err := client.GetJob(ctx, &pb.GetJobRequest{JobId: id})
		if err != nil || !resp.Found {
			fmt.Printf("  %s  ???\n", id)
			continue
		}
		job := resp.Job
		line := fmt.Sprintf("  %s  %-12s", job.Id, job.Status)
		if job.LastError != "" {
			line += fmt.Sprintf("  error=%s", job.LastError)
		}
		fmt.Println(line)
	}
	fmt.Println("\nDemo complete!")
}

// ─────────────────────────────────────────
// Autopsy commands (dead letter forensics)
// ─────────────────────────────────────────

func cmdAutopsy(ctx context.Context, client pb.SchedulerServiceClient, args []string) {
	jobID := parseFlag(args, "--job-id", "")
	tenant := parseFlag(args, "--tenant", "default")
	if jobID == "" {
		fmt.Fprintln(os.Stderr, "Error: --job-id is required")
		os.Exit(1)
	}

	resp, err := client.GetAutopsy(ctx, &pb.GetAutopsyRequest{
		JobId:    jobID,
		TenantId: tenant,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Get autopsy failed: %v\n", err)
		os.Exit(1)
	}

	if !resp.Found {
		fmt.Println("No autopsy report found for this job")
		os.Exit(1)
	}

	// Pretty-print the JSON report
	var pretty json.RawMessage
	if err := json.Unmarshal([]byte(resp.ReportJson), &pretty); err != nil {
		fmt.Println(resp.ReportJson)
		return
	}
	formatted, err := json.MarshalIndent(pretty, "", "  ")
	if err != nil {
		fmt.Println(resp.ReportJson)
		return
	}
	fmt.Println(string(formatted))
}

func cmdAutopsyList(ctx context.Context, client pb.SchedulerServiceClient, args []string) {
	tenant := parseFlag(args, "--tenant", "default")
	limitStr := parseFlag(args, "--limit", "20")
	var limit int32
	fmt.Sscanf(limitStr, "%d", &limit)
	if limit <= 0 {
		limit = 20
	}

	resp, err := client.ListAutopsies(ctx, &pb.ListAutopsiesRequest{
		TenantId: tenant,
		Limit:    limit,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "List autopsies failed: %v\n", err)
		os.Exit(1)
	}

	if len(resp.Autopsies) == 0 {
		fmt.Println("No autopsy reports found")
		return
	}

	fmt.Printf("%-36s %-36s %-25s\n", "AUTOPSY ID", "JOB ID", "CREATED AT")
	fmt.Println(strings.Repeat("-", 100))
	for _, a := range resp.Autopsies {
		fmt.Printf("%-36s %-36s %-25s\n", a.Id, a.JobId, a.CreatedAt)
	}
	fmt.Printf("\nTotal: %d reports\n", len(resp.Autopsies))
}

// ─────────────────────────────────────────
// Schedule commands
// ─────────────────────────────────────────

func cmdScheduleCreate(ctx context.Context, client pb.SchedulerServiceClient, args []string) {
	name := parseFlag(args, "--name", "")
	cronExpr := parseFlag(args, "--cron", "")
	payload := parseFlag(args, "--payload", "")
	tenant := parseFlag(args, "--tenant", "default")
	priority := parseFlag(args, "--priority", "MEDIUM")
	missedPolicy := parseFlag(args, "--missed-policy", "SKIP")

	if name == "" || cronExpr == "" || payload == "" {
		fmt.Fprintln(os.Stderr, "Error: --name, --cron, and --payload are required")
		os.Exit(1)
	}

	resp, err := client.CreateSchedule(ctx, &pb.CreateScheduleRequest{
		TenantId:     tenant,
		Name:         name,
		CronExpr:     cronExpr,
		Payload:      payload,
		Priority:     priority,
		MaxRetries:   3,
		RunTimeoutMs: 300000,
		MissedPolicy: missedPolicy,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Create schedule failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Schedule created:\n")
	fmt.Printf("  ID:     %s\n", resp.ScheduleId)
	fmt.Printf("  Name:   %s\n", name)
	fmt.Printf("  Cron:   %s\n", cronExpr)
	fmt.Printf("  Next:   %s\n", time.UnixMilli(resp.Schedule.NextRunAtMs).Format(time.RFC3339))
}

func cmdScheduleList(ctx context.Context, client pb.SchedulerServiceClient, args []string) {
	tenant := parseFlag(args, "--tenant", "default")

	resp, err := client.ListSchedules(ctx, &pb.ListSchedulesRequest{
		TenantId: tenant,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "List schedules failed: %v\n", err)
		os.Exit(1)
	}

	if len(resp.Schedules) == 0 {
		fmt.Println("No schedules found")
		return
	}

	fmt.Printf("%-36s %-20s %-15s %-8s %-7s %s\n",
		"ID", "NAME", "CRON", "ENABLED", "POLICY", "NEXT RUN")
	for _, s := range resp.Schedules {
		nextRun := time.UnixMilli(s.NextRunAtMs).Format("15:04:05")
		fmt.Printf("%-36s %-20s %-15s %-8v %-7s %s\n",
			s.Id, s.Name, s.CronExpr, s.Enabled, s.MissedPolicy, nextRun)
	}
}

func cmdScheduleToggle(ctx context.Context, client pb.SchedulerServiceClient, args []string, enabled bool) {
	scheduleID := parseFlag(args, "--id", "")
	tenant := parseFlag(args, "--tenant", "default")

	if scheduleID == "" {
		fmt.Fprintln(os.Stderr, "Error: --id is required")
		os.Exit(1)
	}

	resp, err := client.ToggleSchedule(ctx, &pb.ToggleScheduleRequest{
		ScheduleId: scheduleID,
		TenantId:   tenant,
		Enabled:    enabled,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Toggle schedule failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(resp.Message)
}

func cmdScheduleDelete(ctx context.Context, client pb.SchedulerServiceClient, args []string) {
	scheduleID := parseFlag(args, "--id", "")
	tenant := parseFlag(args, "--tenant", "default")

	if scheduleID == "" {
		fmt.Fprintln(os.Stderr, "Error: --id is required")
		os.Exit(1)
	}

	resp, err := client.DeleteSchedule(ctx, &pb.DeleteScheduleRequest{
		ScheduleId: scheduleID,
		TenantId:   tenant,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Delete schedule failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(resp.Message)
}
