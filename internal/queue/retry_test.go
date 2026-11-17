package queue

import (
	"testing"
	"time"
)

func TestRetryPolicy(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 5, BaseDelay: time.Second, MaxDelay: 3 * time.Second}
	if err := policy.Validate(); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		attempt int
		delay   time.Duration
		retry   bool
	}{
		{0, 0, false},
		{1, time.Second, true},
		{2, 2 * time.Second, true},
		{3, 3 * time.Second, true},
		{4, 3 * time.Second, true},
		{5, 0, false},
		{6, 0, false},
	} {
		delay, retry := policy.NextDelay(test.attempt)
		if delay != test.delay || retry != test.retry {
			t.Errorf("attempt %d: got (%s, %t), want (%s, %t)", test.attempt, delay, retry, test.delay, test.retry)
		}
	}
}

func TestRetryPolicyRejectsInvalidSettings(t *testing.T) {
	for _, policy := range []RetryPolicy{
		{MaxAttempts: 0, BaseDelay: time.Second, MaxDelay: time.Second},
		{MaxAttempts: 3, BaseDelay: 0, MaxDelay: time.Second},
		{MaxAttempts: 3, BaseDelay: 2 * time.Second, MaxDelay: time.Second},
	} {
		if err := policy.Validate(); err == nil {
			t.Errorf("accepted invalid policy: %+v", policy)
		}
	}
}
