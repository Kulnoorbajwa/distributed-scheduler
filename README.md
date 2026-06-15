# Distributed Job Scheduler

A fault-tolerant, multi-tenant distributed job scheduler written in Go. A scheduler service accepts jobs over gRPC, persists them in PostgreSQL, and dispatches them to a pool of workers using lease-based ownership. It supports recurring (cron) schedules, automatic retries with exponential backoff, high availability through leader election, and per-tenant isolation and rate limiting.

## Features

- **gRPC API** for submitting, querying, listing, and cancelling jobs
- **Worker pool** with lease-based job ownership and version checking to prevent double execution
- **Recurring schedules** using standard cron expressions, with idempotent firing and missed-run policies
- **Automatic retries** with exponential backoff and jitter, and dead-letter handling for jobs that exhaust their retries
- **Dead-letter forensics** — failure reports with a reconstructed timeline and probable-cause classification, with secrets scrubbed before storage
- **High availability** — multiple scheduler instances elect a leader via PostgreSQL advisory locks; only the leader runs background loops, while all instances serve client requests
- **Multi-tenancy** with per-tenant quotas, queue limits, and rate limiting
- **Observability** — Prometheus metrics and a Grafana dashboard out of the box
- **Health endpoints** for liveness and readiness probes

## Architecture

```
          ┌────────────┐
          │   Client   │  (gRPC: submit / status / list / cancel / schedules)
          └─────┬──────┘
                │
        ┌───────▼────────┐      leader election      ┌────────────────┐
        │   Scheduler    │◄───(advisory lock)───────►│   Scheduler    │
        │   (leader)     │                            │   (standby)    │
        └───┬────────┬───┘                            └────────────────┘
            │        │
   dispatch │        │ persist
            │        ▼
            │   ┌──────────┐
            │   │ Postgres │  jobs, leases, schedules, autopsy reports
            │   └──────────┘
            ▼
   ┌────────┬────────┬────────┐
   │ Worker │ Worker │ Worker │  (execute job payloads, report results)
   └────────┴────────┴────────┘
```

| Component | Responsibility |
|-----------|----------------|
| **Scheduler** | Accepts jobs, manages the queue, dispatches to workers, monitors heartbeats, evaluates schedules, reclaims expired leases |
| **Worker** | Claims dispatched jobs, executes payloads, reports results, renews leases, drains gracefully on shutdown |
| **PostgreSQL** | Durable store for jobs, leases, schedules, and failure reports; backs leader election via advisory locks |
| **Prometheus / Grafana** | Metrics collection and dashboards |

### Job types

Workers execute typed JSON payloads:

```json
{"type": "shell", "command": "echo hello world"}
{"type": "http",  "method": "GET", "url": "https://example.com/health"}
{"type": "sleep", "duration_ms": 5000}
```

## Security

Security was a first-class design goal:

- **Authentication** — separate API and worker tokens, validated by a gRPC interceptor using constant-time comparison
- **Command execution** — shell jobs run against a strict binary allowlist with no shell interpreter, so shell metacharacters and injection are not possible; dangerous arguments and path traversal are rejected
- **SSRF protection** — the HTTP executor validates every resolved IP at dial time, blocking private, loopback, link-local, and cloud-metadata addresses, and restricts schemes and headers
- **Tenant isolation** — every database query is scoped by tenant ID
- **Secret scrubbing** — failure reports are scrubbed of tokens, passwords, and keys before they are persisted
- **Rate limiting** — per-tenant token-bucket limiting, with the tenant derived from the request body rather than spoofable metadata
- **Transport security** — optional TLS on all gRPC connections
- **No hardcoded secrets** — all credentials are supplied through environment variables

## Getting started

### Prerequisites

- Docker and Docker Compose
- Go 1.24+ (only required to build the CLI client locally)

### Configuration

Copy the example environment file and set strong values:

```bash
cp .env.example .env
# edit .env and set POSTGRES_PASSWORD, API_TOKEN, WORKER_TOKEN, GRAFANA_PASSWORD
```

### Run the stack

```bash
docker compose up --build
```

This starts PostgreSQL, two scheduler instances (one leader, one standby), three workers, Prometheus, and Grafana.

| Service | Address |
|---------|---------|
| Scheduler (gRPC) | `localhost:50051` |
| Scheduler metrics | `localhost:9090` |
| Prometheus | `localhost:9091` |
| Grafana | `localhost:3000` |

### Use the CLI

Build the client and point it at the scheduler:

```bash
go build -o client ./client
export API_TOKEN=<the API_TOKEN from your .env>

# Submit a job
./client submit --payload '{"type":"shell","command":"echo hello world"}'

# Check status
./client status --job-id <uuid>

# List jobs
./client list --tenant default

# Create a recurring schedule (hourly)
./client schedule-create --name "hourly" --cron "0 * * * *" \
  --payload '{"type":"shell","command":"echo tick"}'

# Inspect a dead-lettered job
./client autopsy --job-id <uuid>
```

Run `./client` with no arguments to see all commands and options.

## Tech stack

- **Go** with **gRPC** and **Protocol Buffers**
- **PostgreSQL** (via `pgx`) for durable state and leader election
- **Prometheus** and **Grafana** for observability
- **Docker Compose** for local orchestration
- **`robfig/cron`** for schedule evaluation, **`zap`** for structured logging

## Testing

```bash
go test ./...
```

The suite covers command validation, retry backoff, authentication, and rate limiting.
