package profefe

import (
	"context"
	"io"
	"io/ioutil"
	"testing"

	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
)

func TestQuerier_GetProfilesTo_contextCancelled(t *testing.T) {
	list := &unboundProfileList{}
	sr := &storage.StubReader{
		ListProfilesFunc: func(ctx context.Context, _ []profile.ID) (storage.ProfileList, error) {
			return list, nil
		},
	}

	testLogger := log.New(zaptest.NewLogger(t))
	querier := NewQuerier(testLogger, sr)

	// because we cancel the context, the get call below will exit w/o reading the whole (unbound) profile list
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := querier.GetProfilesTo(ctx, ioutil.Discard, []profile.ID{"p1", "p2"})
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
