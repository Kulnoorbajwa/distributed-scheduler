package retry

import (
	"math"
	"math/rand"
	"time"
)

// CalculateBackoff returns an exponential backoff duration with jitter.
// Formula: min(baseDelay * 2^retryCount, maxDelay) +/- 25% jitter
func CalculateBackoff(retryCount int, baseDelay, maxDelay time.Duration) time.Duration {
	exp := math.Pow(2, float64(retryCount))
	delay := time.Duration(float64(baseDelay) * exp)
	if delay > maxDelay {
		delay = maxDelay
	}

	// Add jitter: +/- 25% to prevent thundering herd
	jitterRange := int64(delay) / 4
	if jitterRange > 0 {
		jitter := rand.Int63n(2*jitterRange) - jitterRange
		delay += time.Duration(jitter)
	}

	if delay < 0 {
		delay = baseDelay
	}

	return delay
}
