package queue

import (
	"errors"
	"time"
)

// RetryPolicy caps both the number of attempts and the wait between attempts.
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

func (policy RetryPolicy) Validate() error {
	if policy.MaxAttempts < 1 || policy.BaseDelay <= 0 || policy.MaxDelay < policy.BaseDelay {
		return errors.New("retry policy requires at least one attempt and 0 < base delay <= max delay")
	}
	return nil
}

// NextDelay returns the wait after a failed attempt numbered from one.
// A claim counts as an attempt, including when its worker dies before finishing.
func (policy RetryPolicy) NextDelay(failedAttempt int) (time.Duration, bool) {
	if failedAttempt < 1 || failedAttempt >= policy.MaxAttempts {
		return 0, false
	}

	delay := policy.BaseDelay
	for attempt := 1; attempt < failedAttempt && delay < policy.MaxDelay; attempt++ {
		if delay > policy.MaxDelay/2 {
			return policy.MaxDelay, true
		}
		delay *= 2
	}
	return delay, true
}
