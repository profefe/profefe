package profefe

import (
	"context"
	"io/ioutil"
	"testing"
	"time"

	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
)

func TestQuerier_FindMergeProfileTo_contextCancelled(t *testing.T) {
	sr := &storage.MockReader{
		FindProfileIDsMock: func(ctx context.Context, _ *storage.FindProfilesParams) ([]profile.ID, error) {
			return []profile.ID{profile.NewID()}, nil
		},
		ListProfilesMock: func(ctx context.Context, _ []profile.ID) (storage.ProfileList, error) {
			return &unboundProfileList{}, nil
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
}

// unbound profile list whose Next method always returns true
type unboundProfileList struct{}

func (pl *unboundProfileList) Next() bool {
	return true
}

func (pl *unboundProfileList) Profile() (p *pprofProfile.Profile, err error) { return }
func (pl *unboundProfileList) Close() (err error)                            { return }
