-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Jobs table
CREATE TABLE IF NOT EXISTS jobs (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    request_id       TEXT UNIQUE NOT NULL,  -- idempotency key
    tenant_id        TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'PENDING',
    priority         TEXT NOT NULL DEFAULT 'MEDIUM',
    payload          TEXT NOT NULL,
    max_retries      INT NOT NULL DEFAULT 5,
    retry_count      INT NOT NULL DEFAULT 0,
    last_error       TEXT,
    assigned_worker  TEXT,
    lease_id         UUID,
    lease_expires_at TIMESTAMPTZ,
    lease_version    INT NOT NULL DEFAULT 0,
    run_timeout_ms   BIGINT NOT NULL DEFAULT 300000,
    retry_after      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dispatched_at    TIMESTAMPTZ,
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ
);

-- Workers table
CREATE TABLE IF NOT EXISTS workers (
    id             TEXT PRIMARY KEY,
    tenant_id      TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'ACTIVE',
    address        TEXT NOT NULL,
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    registered_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    active_jobs    INT NOT NULL DEFAULT 0,
    max_jobs       INT NOT NULL DEFAULT 10,
    cpu_slots      INT NOT NULL DEFAULT 4,
    memory_mb      INT NOT NULL DEFAULT 512,
    version        TEXT NOT NULL DEFAULT '1.0.0'
);

-- Leases table
CREATE TABLE IF NOT EXISTS leases (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id     UUID NOT NULL REFERENCES jobs(id),
    worker_id  TEXT NOT NULL REFERENCES workers(id),
    version    INT NOT NULL DEFAULT 1,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    renewed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Audit log — every job state transition recorded here
CREATE TABLE IF NOT EXISTS job_transitions (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id     UUID NOT NULL REFERENCES jobs(id),
    from_state TEXT NOT NULL,
    to_state   TEXT NOT NULL,
    worker_id  TEXT,
    reason     TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tenant quotas table
CREATE TABLE IF NOT EXISTS tenant_quotas (
    tenant_id  TEXT PRIMARY KEY,
    max_jobs      INT NOT NULL DEFAULT 1000,
    max_schedules INT NOT NULL DEFAULT 50,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Schedules table — recurring jobs
CREATE TABLE IF NOT EXISTS schedules (
    id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id      TEXT NOT NULL,
    name           TEXT NOT NULL,
    cron_expr      TEXT NOT NULL,
    payload        TEXT NOT NULL,
    priority       TEXT NOT NULL DEFAULT 'MEDIUM',
    max_retries    INT NOT NULL DEFAULT 3,
    run_timeout_ms BIGINT NOT NULL DEFAULT 300000,
    enabled        BOOLEAN NOT NULL DEFAULT TRUE,
    missed_policy  TEXT NOT NULL DEFAULT 'SKIP',
    last_run_at    TIMESTAMPTZ,
    next_run_at    TIMESTAMPTZ NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(tenant_id, name)
);

-- Autopsy reports — forensic analysis of dead-lettered jobs
CREATE TABLE IF NOT EXISTS autopsy_reports (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id     UUID NOT NULL REFERENCES jobs(id),
    tenant_id  TEXT NOT NULL,
    report     JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_autopsy_job ON autopsy_reports(job_id);
CREATE INDEX IF NOT EXISTS idx_autopsy_tenant ON autopsy_reports(tenant_id);

-- Indexes for performance
-- These make queries fast even with millions of jobs
CREATE INDEX IF NOT EXISTS idx_jobs_status
    ON jobs(status);

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_status
    ON jobs(tenant_id, status);

CREATE INDEX IF NOT EXISTS idx_jobs_priority_created
    ON jobs(priority, created_at)
    WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_jobs_lease_expires
    ON jobs(lease_expires_at)
    WHERE status IN ('DISPATCHED', 'RUNNING');

CREATE INDEX IF NOT EXISTS idx_workers_status
    ON workers(status);

CREATE INDEX IF NOT EXISTS idx_workers_heartbeat
    ON workers(last_heartbeat)
    WHERE status = 'ACTIVE';

CREATE INDEX IF NOT EXISTS idx_leases_job
    ON leases(job_id);

CREATE INDEX IF NOT EXISTS idx_transitions_job
    ON job_transitions(job_id);

CREATE INDEX IF NOT EXISTS idx_schedules_next_run
    ON schedules(next_run_at)
    WHERE enabled = TRUE;

CREATE INDEX IF NOT EXISTS idx_schedules_tenant
    ON schedules(tenant_id);

-- Function to auto-update updated_at on jobs
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER jobs_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();