package baqueue

import (
	"math"
	"math/rand/v2"
)

func ShouldRetry(attempt, maxAttempts int) bool { return attempt < maxAttempts }

// ComputeDelay returns seconds; exponential jitter is [0, 10%) with a 1h cap.
func ComputeDelay(b Backoff, attempt int) float64 {
	attempt = max(1, attempt)
	if b.Delays != nil {
		if len(b.Delays) == 0 {
			return 0
		}
		return b.Delays[min(attempt-1, len(b.Delays)-1)]
	}
	switch b.Strategy {
	case "fixed":
		return 5
	case "linear":
		return math.Min(5*float64(attempt), 3600)
	default:
		delay := 5 * math.Pow(2, float64(min(attempt-1, 20)))
		return math.Min(delay*(1+rand.Float64()*0.1), 3600)
	}
}
