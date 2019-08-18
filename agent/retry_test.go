package agent

import (
	"errors"
	"testing"
	"time"
)

func TestDo(t *testing.T) {
	errTest := errors.New("test error")

	t.Run("Do", func(t *testing.T) {
		err := Do(time.Millisecond, 5*time.Millisecond, func() error {
			return nil
		})
		if err != nil {
			t.Errorf("Do: unexpected %v", err)
		}
	})

	t.Run("Do with failed attempts", func(t *testing.T) {
		var (
			attempts    int32
			maxAttempts int32 = 2
		)
		err := Do(time.Millisecond, 5*time.Millisecond, func() error {
			attempts += 1
			if attempts == maxAttempts {
				return nil
			}
			return errTest
		})
		if attempts != maxAttempts {
			t.Errorf("Do: got %d attempts, want %d", attempts, maxAttempts)
		}
		if err != nil {
			t.Errorf("Do: unexpected %v", err)
		}
	})

	t.Run("Do with cancel", func(t *testing.T) {
		var (
			attempts    int32
			maxAttempts int32 = 2
		)
		err := Do(time.Millisecond, 5*time.Millisecond, func() error {
			attempts += 1
			if attempts == maxAttempts {
				return Cancel(errTest)
			}
			return errors.New("unable to do")
		})
		if attempts != maxAttempts {
			t.Errorf("Do: got %d attempts, want %d", attempts, maxAttempts)
		}
		if err == nil || err.Error() != errTest.Error() {
			t.Errorf("Do: got error %v, want %v", err, errTest)
		}
	})
}
