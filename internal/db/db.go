package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DB wraps the connection pool and gives us a single place
// to add logging, metrics, and tracing later
type DB struct {
	Pool *pgxpool.Pool
}

// New creates a new database connection pool
func New(ctx context.Context, databaseURL string) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database URL: %w", err)
	}

	// Connection pool settings
	cfg.MaxConns = 25                       // max open connections
	cfg.MinConns = 5                        // keep 5 connections warm
	cfg.MaxConnLifetime = 1 * time.Hour     // recycle connections hourly
	cfg.MaxConnIdleTime = 30 * time.Minute  // close idle connections after 30min
	cfg.HealthCheckPeriod = 1 * time.Minute // ping idle connections to keep alive

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Verify connection is actually working
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &DB{Pool: pool}, nil
}

// Close shuts down the connection pool cleanly
func (db *DB) Close() {
	db.Pool.Close()
}

// HealthCheck verifies the database is reachable
func (db *DB) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := db.Pool.Ping(ctx); err != nil {
		return fmt.Errorf("database health check failed: %w", err)
	}
	return nil
}
