package agent

import (
	"time"
)

type backoff struct {
	MinDelay    time.Duration
	MaxDelay    time.Duration
	MaxAttempts int
}

func newBackoff(minDelay, maxDelay time.Duration, attempts int) *backoff {
	return &backoff{
		MinDelay:    minDelay,
		MaxDelay:    maxDelay,
		MaxAttempts: attempts,
	}
}

func (r *backoff) Do(fn func() error) (err error) {
	ckeckOk, err := r.Check(fn())
	if !ckeckOk {
		return err
	}

	for attempt := 1; r.MaxAttempts == 0 || attempt < r.MaxAttempts; attempt++ {
		delay := r.backoff(attempt)
		time.Sleep(delay)

		err = fn()
		ckeckOk, err := r.Check(err)
		if !ckeckOk {
			return err
		}
	}
	return err
}

func (r *backoff) Check(err error) (bool, error) {
	if err == nil {
		return false, nil
	} else if sErr, ok := err.(*cancelRetryError); ok {
		return false, sErr
	}
	return true, err
}

func (r *backoff) backoff(attempt int) time.Duration {
	n := attempt * int(r.MinDelay)
	delay := time.Duration(n)
	if delay > r.MaxDelay {
		delay = r.MaxDelay
	}
	return delay
}

type cancelRetryError struct {
	Err error
}

func Cancel(err error) error {
	return &cancelRetryError{err}
}

func (e *cancelRetryError) Error() string {
	return e.Err.Error()
}
