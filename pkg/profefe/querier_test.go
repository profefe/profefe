package profefe

import (
	"context"
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
)

func TestQuerier_FindMergeProfileTo_contextCancelled(t *testing.T) {
	list := &unboundProfileList{}
	sr := &storage.MockReader{
		FindProfileIDsMock: func(ctx context.Context, _ *storage.FindProfilesParams) ([]profile.ID, error) {
			return []profile.ID{profile.NewID()}, nil
		},
		ListProfilesMock: func(ctx context.Context, _ []profile.ID) (storage.ProfileList, error) {
			return list, nil
		},
	}

	testLogger := log.New(zaptest.NewLogger(t))
	querier := NewQuerier(testLogger, sr)

	// because we cancel the context, the find call below will exit w/o reading the whole (unbound) profile list
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	params := &storage.FindProfilesParams{
		Service:      "test-server",
		CreatedAtMin: time.Now(),
	}
	err := querier.FindMergeProfileTo(ctx, ioutil.Discard, params)
	assert.Equal(t, context.Canceled, err)

	assert.True(t, list.closed, "profile list must be closed")
}

// unbound profile list whose Next method always returns true
type unboundProfileList struct {
	closed bool
}

func (pl *unboundProfileList) Next() bool {
	return true
}

func (pl *unboundProfileList) Close() error {
	pl.closed = true
	return nil
}

func (pl *unboundProfileList) Profile() (pr io.Reader, err error) { return }
