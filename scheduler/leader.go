package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// advisoryLockID is a non-trivial, application-specific lock ID.
// Using a large prime avoids accidental collision with other PostgreSQL
// advisory lock users on the same database.
const advisoryLockID int64 = 7629384105917263

// LeaderElector manages leader election using PostgreSQL session-scoped
// advisory locks. Only one scheduler instance can hold the lock at a time.
// Session-scoped locks auto-release when the database connection drops,
// preventing split-brain: if the leader crashes, the lock is freed
// immediately without requiring explicit unlock.
type LeaderElector struct {
	pool     *pgxpool.Pool
	log      *zap.Logger
	pollInterval time.Duration

	isLeader atomic.Bool
	mu       sync.RWMutex
	onPromote  func() // called when this instance becomes leader
	onDemote   func() // called when this instance loses leadership

	// The dedicated connection holding the advisory lock.
	// nil when not leader.
	lockConn *pgxpool.Conn
}

// NewLeaderElector creates a new leader elector.
// onPromote is called (once) when this instance acquires leadership.
// onDemote is called (once) when leadership is lost.
func NewLeaderElector(pool *pgxpool.Pool, log *zap.Logger, onPromote, onDemote func()) *LeaderElector {
	return &LeaderElector{
		pool:         pool,
		log:          log,
		pollInterval: 5 * time.Second,
		onPromote:    onPromote,
		onDemote:     onDemote,
	}
}

// IsLeader returns whether this instance currently holds the leader lock.
func (le *LeaderElector) IsLeader() bool {
	return le.isLeader.Load()
}

// Run starts the leader election loop. It blocks until ctx is cancelled.
func (le *LeaderElector) Run(ctx context.Context) {
	le.log.Info("leader election started",
		zap.Int64("lock_id", advisoryLockID),
		zap.Duration("poll_interval", le.pollInterval),
	)

	ticker := time.NewTicker(le.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			le.releaseLock()
			return
		case <-ticker.C:
			if le.isLeader.Load() {
				le.verifyLock(ctx)
			} else {
				le.tryAcquireLock(ctx)
			}
		}
	}
}

// tryAcquireLock attempts to acquire the advisory lock (non-blocking).
func (le *LeaderElector) tryAcquireLock(ctx context.Context) {
	// Acquire a dedicated connection from the pool
	conn, err := le.pool.Acquire(ctx)
	if err != nil {
		le.log.Warn("failed to acquire connection for leader election", zap.Error(err))
		leaderElectionErrors.Inc()
		return
	}

	// Try to acquire session-scoped advisory lock (non-blocking)
	var acquired bool
	err = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", advisoryLockID).Scan(&acquired)
	if err != nil {
		conn.Release()
		le.log.Warn("failed to try advisory lock", zap.Error(err))
		leaderElectionErrors.Inc()
		return
	}

	if !acquired {
		conn.Release()
		leaderIsLeader.Set(0)
		return
	}

	// We got the lock!
	le.mu.Lock()
	le.lockConn = conn
	le.isLeader.Store(true)
	le.mu.Unlock()

	leaderIsLeader.Set(1)
	leaderPromotions.Inc()
	le.log.Info("promoted to LEADER — starting background loops")

	if le.onPromote != nil {
		le.onPromote()
	}
}

// verifyLock checks that we still hold the advisory lock.
// If the connection dropped, the lock is gone.
func (le *LeaderElector) verifyLock(ctx context.Context) {
	le.mu.RLock()
	conn := le.lockConn
	le.mu.RUnlock()

	if conn == nil {
		le.demote("lock connection is nil")
		return
	}

	// Ping the connection to verify it's still alive
	err := conn.Ping(ctx)
	if err != nil {
		le.demote("lock connection lost: " + err.Error())
		return
	}

	// Verify we still hold the lock by checking pg_locks
	var held bool
	err = conn.QueryRow(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM pg_locks
			WHERE locktype = 'advisory'
			  AND classid = ($1::bigint >> 32)::int
			  AND objid = ($1::bigint & 4294967295)::int
			  AND pid = pg_backend_pid()
		)`, advisoryLockID,
	).Scan(&held)
	if err != nil || !held {
		le.demote("advisory lock no longer held")
		return
	}
}

// demote transitions from leader to standby.
func (le *LeaderElector) demote(reason string) {
	le.log.Warn("demoted from LEADER to STANDBY", zap.String("reason", reason))

	le.mu.Lock()
	le.isLeader.Store(false)
	if le.lockConn != nil {
		le.lockConn.Release()
		le.lockConn = nil
	}
	le.mu.Unlock()

	leaderIsLeader.Set(0)
	leaderDemotions.Inc()

	if le.onDemote != nil {
		le.onDemote()
	}
}

// releaseLock explicitly releases the advisory lock on shutdown.
func (le *LeaderElector) releaseLock() {
	le.mu.Lock()
	defer le.mu.Unlock()

	if le.lockConn != nil {
		// Explicitly release the lock before dropping the connection
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = le.lockConn.Exec(ctx, "SELECT pg_advisory_unlock($1)", advisoryLockID)
		le.lockConn.Release()
		le.lockConn = nil
	}

	le.isLeader.Store(false)
	leaderIsLeader.Set(0)
	le.log.Info("leader lock released")
}
