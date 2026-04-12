package retry

import (
	"testing"
	"time"
)

func TestCalculateBackoff_ExponentialGrowth(t *testing.T) {
	base := 2 * time.Second
	max := 60 * time.Second

	prev := time.Duration(0)
	for i := 0; i < 5; i++ {
		d := CalculateBackoff(i, base, max)
		// Allow 25% jitter variance, but the median should grow
		// At retry 0: ~2s, retry 1: ~4s, retry 2: ~8s, etc.
		expectedBase := base * time.Duration(1<<uint(i))
		if expectedBase > max {
			expectedBase = max
		}
		minExpected := expectedBase * 3 / 4 // -25%
		maxExpected := expectedBase * 5 / 4 // +25%

		if d < minExpected || d > maxExpected {
			t.Errorf("retry %d: got %v, want between %v and %v", i, d, minExpected, maxExpected)
		}
		if i > 0 && d <= prev/2 {
			// Should generally grow (allowing for jitter)
			t.Logf("retry %d: %v (prev: %v) — growth check (soft)", i, d, prev)
		}
		prev = d
	}
}

func TestCalculateBackoff_CapsAtMax(t *testing.T) {
	base := 2 * time.Second
	max := 10 * time.Second

	// Retry 10 would be 2*1024 = 2048s without cap
	d := CalculateBackoff(10, base, max)
	maxWithJitter := max * 5 / 4 // +25%
	if d > maxWithJitter {
		t.Errorf("retry 10: got %v, exceeds max %v (with jitter %v)", d, max, maxWithJitter)
	}
}

func TestCalculateBackoff_NonNegative(t *testing.T) {
	base := 1 * time.Second
	max := 60 * time.Second

	for i := 0; i < 100; i++ {
		d := CalculateBackoff(0, base, max)
		if d < 0 {
			t.Errorf("got negative duration: %v", d)
		}
	}
}

func TestCalculateBackoff_ZeroRetry(t *testing.T) {
	base := 2 * time.Second
	max := 60 * time.Second

	d := CalculateBackoff(0, base, max)
	// Should be around 2s +/- 25%
	if d < 1500*time.Millisecond || d > 2500*time.Millisecond {
		t.Errorf("retry 0: got %v, want ~2s", d)
	}
}
