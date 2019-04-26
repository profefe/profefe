package retry

import (
	"time"
)

func Do(minDelay, maxDelay time.Duration, fn func() error) error {
	return DoAttempts(minDelay, maxDelay, 0, fn)
}

func DoAttempts(minDelay, maxDelay time.Duration, attempts int, fn func() error) error {
	r := New(minDelay, maxDelay, attempts)
	return r.Do(fn)
}

type Retry struct {
	MinDelay    time.Duration
	MaxDelay    time.Duration
	MaxAttempts int
}

func New(minDelay, maxDelay time.Duration, attempts int) *Retry {
	return &Retry{
		MinDelay:    minDelay,
		MaxDelay:    maxDelay,
		MaxAttempts: attempts,
	}
}

func (r *Retry) Do(fn func() error) (err error) {
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

func (r *Retry) Check(err error) (bool, error) {
	if err == nil {
		return false, nil
	} else if sErr, ok := err.(*cancelRetryError); ok {
		return false, sErr
	}
	return true, err
}

func (r *Retry) backoff(attempt int) time.Duration {
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

func (e *cancelRetryError) Unwrap() error {
	return e.Err
}

func (e *cancelRetryError) Error() string {
	return e.Err.Error()
}
