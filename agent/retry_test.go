package agent

import (
	"errors"
	"testing"
	"time"
)

func TestDo(t *testing.T) {
	errTest := errors.New("test error")

	t.Run("DoRetry", func(t *testing.T) {
		err := DoRetry(time.Millisecond, 5*time.Millisecond, func() error {
			return nil
		})
		if err != nil {
			t.Errorf("DoRetry: unexpected %v", err)
		}
	})

	t.Run("DoRetry with failed attempts", func(t *testing.T) {
		var (
			attempts    int32
			maxAttempts int32 = 2
		)
		err := DoRetry(time.Millisecond, 5*time.Millisecond, func() error {
			attempts += 1
			if attempts == maxAttempts {
				return nil
			}
			return errTest
		})
		if attempts != maxAttempts {
			t.Errorf("DoRetry: got %d attempts, want %d", attempts, maxAttempts)
		}
		if err != nil {
			t.Errorf("DoRetry: unexpected %v", err)
		}
	})

	t.Run("DoRetry with cancel", func(t *testing.T) {
		var (
			attempts    int32
			maxAttempts int32 = 2
		)
		err := DoRetry(time.Millisecond, 5*time.Millisecond, func() error {
			attempts += 1
			if attempts == maxAttempts {
				return Cancel(errTest)
			}
			return errors.New("unable to do")
		})
		if attempts != maxAttempts {
			t.Errorf("DoRetry: got %d attempts, want %d", attempts, maxAttempts)
		}
		if err == nil || err.Error() != errTest.Error() {
			t.Errorf("DoRetry: got error %v, want %v", err, errTest)
		}
	})
}
