# Distributed Job Scheduler — Development Status

## Overview
Production-grade distributed job scheduler built in Go with gRPC, PostgreSQL, Docker Compose, lease-based ownership, multi-tenancy, and security hardening.

---

## Phase 1: Bugs, Security Hardening, Tests — COMPLETE

### Bug Fixes
| Item | Details | Files |
|------|---------|-------|
| UUID→TEXT schema fix | `workers.id` and `leases.worker_id` changed from UUID to TEXT to match actual worker IDs | `schema.sql`, `worker.go` |
| Retry backoff | Exponential backoff with jitter (was missing — jobs retried immediately). Added `retry_after` column, `CalculateBackoff()` function | `internal/retry/backoff.go`, `schema.sql`, `jobstore.go`, `models.go` |

### Security Fixes
| Item | Severity | Details | Files |
|------|----------|---------|-------|
| Timing attack | High | Token comparison replaced with `crypto/subtle.ConstantTimeCompare` in both scheduler and worker auth interceptors | `auth.go`, `worker.go` |
| Race condition | High | Added `sync.Mutex` to protect concurrent access to `activeJobs`/`cancelFuncs` maps in worker (gRPC handlers + execution goroutines) | `worker.go` |
| Rate limit bypass | Medium | Tenant ID now extracted from gRPC request body instead of spoofable `x-tenant-id` metadata header | `ratelimit.go` |
| TLS support | Medium | Conditionally wired up across scheduler server, worker server, worker→scheduler client, and CLI client. Self-signed cert generation script included | `scheduler.go`, `worker.go`, `client/main.go`, `docker/scripts/gen-certs.sh` |
| Graceful drain | Medium | On SIGTERM, worker notifies scheduler it's DRAINING, waits up to 60s for active jobs to finish, then stops. Scheduler won't dispatch new jobs to draining workers | `worker.go`, `scheduler.go` |

### Test Suite (20 tests, all passing)
| File | Tests | What's Covered |
|------|-------|----------------|
| `worker/executor_test.go` | 7 | Command allowlist, blocked binaries, blocked args, path traversal, empty command, path stripping, truncation |
| `internal/retry/backoff_test.go` | 4 | Exponential growth, max cap, non-negative, zero retry |
| `scheduler/auth_test.go` | 8 | Valid API/worker tokens, invalid token, missing token, missing metadata, wrong token type on client RPC, unknown method, raw token without Bearer prefix |
| `scheduler/ratelimit_test.go` | 5 | Burst allowance, over-burst rejection, per-tenant isolation, refill over time, new tenant bucket |

---

## Phase 2: Recurring Jobs (Cron Schedules) — COMPLETE

### New Files
- `internal/db/schedulestore.go` — CRUD with `FOR UPDATE SKIP LOCKED` for safe concurrent access
- `scheduler/cron.go` — Background evaluator loop (every 30s), missed-run policy handling

### Schema
- `schedules` table with cron expression, payload, priority, missed-run policy, tenant isolation
- `max_schedules` column added to `tenant_quotas`
- Indexes on `next_run_at` (for due schedule queries) and `tenant_id`

### RPCs and CLI
- `CreateSchedule`, `ListSchedules`, `ToggleSchedule`, `DeleteSchedule` — all registered in auth interceptor and rate limiter
- CLI: `schedule-create`, `schedule-list`, `schedule-pause`, `schedule-resume`, `schedule-delete`

### Security
- Cron expressions validated server-side; sub-minute intervals rejected
- Per-tenant schedule quota (max 50, configurable in `tenant_quotas`)
- Idempotent job firing: `request_id = schedule:{id}:{nextRunAt.Unix()}`
- Missed-run catchup capped at 10 to prevent schedule bombs
- Payload size validation at schedule creation time

---

## Phase 3: Job Autopsy (Dead Letter Forensics) — COMPLETE

### Features
- `autopsy_reports` table (JSONB report, tenant-scoped)
- `internal/autopsy/scrubber.go` — Scrubs Bearer tokens, passwords, API keys, AWS keys, credentials in URLs, long base64 strings
- `internal/autopsy/report.go` — Builds timeline from `job_transitions`, classifies probable cause with heuristics
- `internal/autopsy/scrubber_test.go` — 8 tests covering all secret patterns + false positive check
- `internal/db/autopsystore.go` — Tenant-scoped queries (GetByJobID, List) with max cap (100 results)
- Proto messages + RPCs (`GetAutopsy`, `ListAutopsies`) defined and generated
- Scheduler auto-generates autopsy report when a job transitions to `DEAD_LETTER`
- Auth interceptor and rate limiter updated for new RPCs
- CLI commands: `autopsy --job-id <id>` (pretty-printed JSON report), `autopsy-list --tenant <id> --limit N`

### Security Hardening
- Transitions query capped at 100 rows to prevent unbounded report size
- List autopsies capped at 100 results server-side (prevents client abuse)
- All report content scrubbed for secrets before storage

### Probable Cause Heuristics
| Pattern | Classification |
|---------|---------------|
| All errors contain "timeout"/"deadline exceeded" | "job consistently exceeds run timeout" |
| All errors contain "connection refused"/"no such host" | "target service unavailable" |
| All errors contain "command rejected"/"not in allowlist" | "payload validation failure — job will never succeed" |
| All errors identical | "persistent failure — same error on every attempt" |
| All attempts on same worker | "worker-specific issue" |
| Errors vary across workers | "intermittent failure — possibly flaky" |

---

## Remaining Phases

### Phase 4: Scheduler HA (Leader Election) — COMPLETE

#### New Files
- `scheduler/leader.go` — LeaderElector with `pg_try_advisory_lock`, promote/demote callbacks, lock verification

#### Features
- PostgreSQL session-scoped advisory lock-based leader election
- Only leader runs dispatch, heartbeat monitor, reconciler, cron evaluator, metrics updater
- All instances serve client RPCs (stateless DB reads/writes)
- Standby polls every 5s, promotes within one cycle when lock available
- On promotion: reclaims expired jobs before starting background loops
- On demotion: cancels all background loops, transitions to standby
- Health endpoints: `/health` (liveness — DB reachable), `/ready` (readiness — includes leader status)
- Second scheduler instance (`scheduler-standby`) in `docker-compose.yml`
- Prometheus scrapes both scheduler instances

#### Security
- Non-trivial advisory lock ID (`7629384105917263`) prevents accidental collision with other DB users
- Session-scoped locks auto-release on connection drop — prevents split-brain
- Leader periodically verifies lock via `pg_locks` + connection ping
- Explicit unlock on graceful shutdown for faster failover
- Health endpoints do NOT expose internal error details (logged server-side only)
- Metrics: `scheduler_leader_is_leader`, `scheduler_leader_promotions_total`, `scheduler_leader_demotions_total`, `scheduler_leader_election_errors_total`

### Phase 5: Job DAGs (Dependencies)
- `job_dependencies` table with cycle detection at submission time (DFS)
- Dependency-aware dispatch: jobs only claimable when all upstream jobs `SUCCEEDED`
- Cascade cancellation when upstream job fails/dead-letters
- Fan-out (A→B+C) and fan-in (B+C→D) support
- `SubmitDAG` RPC with `DAGNode` messages
- CLI: `submit --depends-on job-id-1,job-id-2`

### Phase 6: Polish & Ship
- `Makefile` with build, test, proto, docker, demo, lint targets
- Thin REST API (`POST /api/v1/jobs`, etc.) using standard library
- `scripts/demo.sh` — automated end-to-end demo
- Additional Prometheus metrics for schedules, autopsies, leader elections, DAGs
- Updated Grafana dashboard

---

## Security Posture

### Implemented Protections
| Category | Protection | Implementation |
|----------|-----------|----------------|
| Authentication | Separate API and worker tokens | gRPC metadata interceptor |
| Token comparison | Constant-time comparison | `crypto/subtle.ConstantTimeCompare` |
| Rate limiting | Per-tenant token bucket | 50 req/s, burst 100, tenant from request body |
| Command execution | Allowlist + blocked args | No shell invocation (`exec.Command`), no interpreters |
| SSRF prevention | IP validation at dial time | Blocks private, loopback, link-local, metadata IPs; no TOCTOU |
| HTTP security | Scheme + header restrictions | Only http/https; blocked headers (Host, Proxy-Authorization, etc.) |
| Data isolation | Tenant-scoped queries | All DB queries filter by `tenant_id` |
| Secret protection | Regex scrubbing | Autopsy reports scrub tokens, passwords, API keys before storage |
| Concurrency safety | Mutex on shared maps | `sync.Mutex` on worker's `activeJobs`/`cancelFuncs` |
| Transport security | TLS support | Conditional TLS on all gRPC connections |
| Graceful shutdown | Drain before stop | Worker notifies scheduler, waits for active jobs |
| Idempotency | Request ID dedup | `ON CONFLICT DO NOTHING` on job submission |
| Lease safety | Version checking | Stale lease detection prevents double-execution |
| Schedule safety | Quota + interval limits | Per-tenant quota (50), minimum 1-minute interval, catchup cap (10) |

### Known Improvements to Make
| Issue | Severity | When to Address |
|-------|----------|-----------------|
| ~~Autopsy report size — unbounded transition query could create huge reports~~ | ~~Medium~~ | **FIXED** — capped at 100 transitions |
| Schedule payload validation — not parsed as valid job type at creation time | Medium | Phase 6 — validate JSON structure |
| ~~Advisory lock ID predictability — other DB users could interfere~~ | ~~Low~~ | **FIXED** — using non-trivial lock ID `7629384105917263` |
| DAG depth bomb — unbounded dependency chains could exhaust resources | High | Phase 5 — enforce max 100 nodes, depth 10 |
| REST API auth — needs same token validation as gRPC | High | Phase 6 — Authorization header middleware |
| Log injection — error messages could contain escape sequences or be very long | Low | Ongoing — truncate in ReportResult handler |
| ~~Idle DB connections — standby scheduler holds full connection pool~~ | ~~Low~~ | **ADDRESSED** — all instances serve RPCs, so pool is justified; not purely idle |

---

## File Inventory

### New Files Created
```
internal/retry/backoff.go          — Exponential backoff with jitter
internal/retry/backoff_test.go     — Backoff unit tests
internal/autopsy/scrubber.go       — Secret scrubbing for autopsy reports
internal/autopsy/report.go         — Autopsy report generation + cause classification
internal/autopsy/scrubber_test.go  — Scrubber + classification tests
internal/db/schedulestore.go       — Schedule CRUD operations
internal/db/autopsystore.go        — Autopsy report queries
scheduler/cron.go                  — Cron evaluation loop for recurring jobs
scheduler/auth_test.go             — Auth interceptor tests
scheduler/ratelimit_test.go        — Rate limiter tests
worker/executor_test.go            — Command validation + truncation tests
docker/scripts/gen-certs.sh        — Self-signed TLS cert generation
scheduler/leader.go                — PostgreSQL advisory lock leader election
```

### Modified Files
```
internal/db/schema.sql             — UUID→TEXT, retry_after, schedules table, autopsy table, indexes
internal/db/jobstore.go            — Retry backoff in FailJob, retry_after in ClaimNextJob/scanJob
internal/models/models.go          — RetryAfter field, Schedule struct, MissedRunPolicy
scheduler/scheduler.go             — TLS, drain support, schedule RPCs, autopsy RPCs, cron startup
scheduler/auth.go                  — Constant-time compare, new RPC registrations
scheduler/ratelimit.go             — Tenant from request body, new RPC support
scheduler/metrics.go               — Leader election metrics added
worker/worker.go                   — Mutex, TLS, drain, constant-time compare
client/main.go                     — TLS, schedule + autopsy CLI commands
proto/scheduler.proto              — Schedule + autopsy messages and RPCs
docker-compose.yml                 — Second scheduler instance (scheduler-standby)
docker/prometheus.yml              — Scrapes both scheduler instances
```
