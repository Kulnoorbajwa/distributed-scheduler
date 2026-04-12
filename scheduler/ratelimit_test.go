package main

import (
	"testing"
	"time"
)

func TestRateLimiter_AllowsBurst(t *testing.T) {
	rl := newRateLimiter(10, 5, 1*time.Minute)

	// Should allow up to burst (5) requests immediately
	for i := 0; i < 5; i++ {
		if !rl.allow("tenant-a") {
			t.Errorf("request %d should be allowed within burst", i)
		}
	}
}

func TestRateLimiter_RejectsOverBurst(t *testing.T) {
	rl := newRateLimiter(10, 3, 1*time.Minute)

	// Exhaust burst
	for i := 0; i < 3; i++ {
		rl.allow("tenant-a")
	}

	// Next request should be rejected (no time for refill)
	if rl.allow("tenant-a") {
		t.Error("request should be rejected after burst exhausted")
	}
}

func TestRateLimiter_PerTenantIsolation(t *testing.T) {
	rl := newRateLimiter(10, 3, 1*time.Minute)

	// Exhaust tenant-a's burst
	for i := 0; i < 3; i++ {
		rl.allow("tenant-a")
	}

	// tenant-b should still have full burst
	if !rl.allow("tenant-b") {
		t.Error("tenant-b should not be affected by tenant-a's rate limit")
	}
}

func TestRateLimiter_RefillsOverTime(t *testing.T) {
	rl := newRateLimiter(1000, 1, 1*time.Minute) // 1000/sec, burst 1

	// Exhaust the single token
	rl.allow("tenant-a")

	// Should be rejected immediately
	if rl.allow("tenant-a") {
		t.Error("should be rejected immediately after exhausting token")
	}

	// Wait enough time for at least 1 token to refill
	time.Sleep(5 * time.Millisecond)

	// Should be allowed now (1000/sec = 1 token per ms, 5ms = 5 tokens)
	if !rl.allow("tenant-a") {
		t.Error("should be allowed after refill time")
	}
}

func TestRateLimiter_UnknownTenantGetsNewBucket(t *testing.T) {
	rl := newRateLimiter(10, 5, 1*time.Minute)

	// First request from a new tenant should succeed
	if !rl.allow("new-tenant") {
		t.Error("first request from new tenant should be allowed")
	}
}
