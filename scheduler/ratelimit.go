package main

import (
	"context"
	"sync"
	"time"

	pb "github.com/Kulnoorbajwa/distributed-scheduler/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// tokenBucket implements a per-tenant token bucket rate limiter.
type tokenBucket struct {
	tokens     float64
	maxTokens  float64
	refillRate float64   // tokens per second
	lastRefill time.Time
}

func (tb *tokenBucket) allow() bool {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens += elapsed * tb.refillRate
	if tb.tokens > tb.maxTokens {
		tb.tokens = tb.maxTokens
	}
	tb.lastRefill = now

	if tb.tokens >= 1 {
		tb.tokens--
		return true
	}
	return false
}

// rateLimiter tracks per-tenant request rates.
type rateLimiter struct {
	mu          sync.Mutex
	buckets     map[string]*tokenBucket
	rate        float64 // tokens per second
	burst       float64 // max bucket size
	cleanupTick time.Duration
}

func newRateLimiter(perSecond, burst int, cleanupInterval time.Duration) *rateLimiter {
	rl := &rateLimiter{
		buckets:     make(map[string]*tokenBucket),
		rate:        float64(perSecond),
		burst:       float64(burst),
		cleanupTick: cleanupInterval,
	}
	go rl.cleanup()
	return rl
}

// allow checks whether a request from the given tenant is allowed.
func (rl *rateLimiter) allow(tenant string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	tb, ok := rl.buckets[tenant]
	if !ok {
		tb = &tokenBucket{
			tokens:     rl.burst,
			maxTokens:  rl.burst,
			refillRate: rl.rate,
			lastRefill: time.Now(),
		}
		rl.buckets[tenant] = tb
	}
	return tb.allow()
}

// cleanup removes idle buckets periodically to prevent memory leaks.
func (rl *rateLimiter) cleanup() {
	ticker := time.NewTicker(rl.cleanupTick)
	defer ticker.Stop()

	for range ticker.C {
		rl.mu.Lock()
		now := time.Now()
		for tenant, tb := range rl.buckets {
			// If bucket is full and hasn't been touched in a while, remove it
			if now.Sub(tb.lastRefill) > rl.cleanupTick {
				delete(rl.buckets, tenant)
			}
		}
		rl.mu.Unlock()
	}
}

// tenantFromContext extracts the tenant ID from gRPC metadata.
// Falls back to "unknown" if not present (auth interceptor runs first).
func tenantFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "unknown"
	}
	tenants := md.Get("x-tenant-id")
	if len(tenants) == 0 {
		return "unknown"
	}
	return tenants[0]
}

// tenantFromRequest extracts tenant_id from the request body when possible,
// falling back to the x-tenant-id metadata header. This prevents clients
// from spoofing a different tenant header to bypass rate limits.
func tenantFromRequest(ctx context.Context, req interface{}) string {
	switch r := req.(type) {
	case *pb.SubmitJobRequest:
		if r.TenantId != "" {
			return r.TenantId
		}
	case *pb.GetJobRequest:
		if r.TenantId != "" {
			return r.TenantId
		}
	case *pb.CancelJobClientRequest:
		if r.TenantId != "" {
			return r.TenantId
		}
	case *pb.ListJobsRequest:
		if r.TenantId != "" {
			return r.TenantId
		}
	case *pb.CreateScheduleRequest:
		if r.TenantId != "" {
			return r.TenantId
		}
	case *pb.ListSchedulesRequest:
		if r.TenantId != "" {
			return r.TenantId
		}
	case *pb.ToggleScheduleRequest:
		if r.TenantId != "" {
			return r.TenantId
		}
	case *pb.DeleteScheduleRequest:
		if r.TenantId != "" {
			return r.TenantId
		}
	case *pb.GetAutopsyRequest:
		if r.TenantId != "" {
			return r.TenantId
		}
	case *pb.ListAutopsiesRequest:
		if r.TenantId != "" {
			return r.TenantId
		}
	}
	return tenantFromContext(ctx)
}

// rateLimitInterceptor returns a gRPC unary interceptor that enforces
// per-tenant rate limits on client-facing RPCs.
func rateLimitInterceptor(rl *rateLimiter) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		// Only rate-limit client-facing RPCs
		if !clientRPCs[info.FullMethod] {
			return handler(ctx, req)
		}

		tenant := tenantFromRequest(ctx, req)
		if !rl.allow(tenant) {
			return nil, status.Errorf(codes.ResourceExhausted,
				"rate limit exceeded for tenant %q — try again shortly", tenant)
		}

		return handler(ctx, req)
	}
}
