package storagetest

import (
	"context"
	"fmt"
	"testing"
	"time"

	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/pprofutil"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type WriterTestSuite struct {
	suite.Suite

	Reader storage.Reader
	Writer storage.Writer
}

func (ts *WriterTestSuite) TestWriteProfile() {
	service := genServiceName()
	createdAt := time.Now()
	wparams := &storage.WriteProfileParams{
		Service:   service,
		Type:      profile.TypeCPU,
		Labels:    profile.Labels{{"key1", "val1"}},
		CreatedAt: createdAt,
	}
	meta, data := WriteProfile(ts.T(), ts.Writer, wparams, "../../../testdata/collector_cpu_1.prof")

	wantPP, err := pprofProfile.ParseData(data)
	ts.Require().NoError(err)

	list, err := ts.Reader.ListProfiles(context.Background(), []profile.ID{meta.ProfileID})
	ts.Require().NoError(err)

	ts.T().Cleanup(func() {
		ts.Require().NoError(list.Close())
	})

	ts.True(list.Next())

	ppr, err := list.Profile()
	ts.Require().NoError(err)

	gotPP, err := pprofProfile.Parse(ppr)
	ts.Require().NoError(err)
	ts.True(pprofutil.ProfilesEqual(wantPP, gotPP))

	ts.False(list.Next())
}

type ReaderTestSuite struct {
	suite.Suite

	Reader storage.Reader
	Writer storage.Writer
}

func (ts *ReaderTestSuite) TestFindProfileIDs() {
	testFindProfileIDs(ts.T(), ts.Reader, ts.Writer)
}

func (ts *ReaderTestSuite) TestListProfiles() {
	testListProfiles(ts.T(), ts.Reader, ts.Writer)
}

func (ts *ReaderTestSuite) TestListServices() {
	testListServices(ts.T(), ts.Reader, ts.Writer)
}

func testFindProfileIDs(t *testing.T, sr storage.Reader, sw storage.Writer) {
	service1 := genServiceName()
	service2 := genServiceName()

	for n := 1; n <= 2; n++ {
		fileName := fmt.Sprintf("../../../testdata/collector_cpu_%d.prof", n)
		params := &storage.WriteProfileParams{
			Service: service1,
			Type:    profile.TypeCPU,
			Labels:  profile.Labels{{"key1", "val1"}},
		}
		WriteProfile(t, sw, params, fileName)
	}

	// a profile of different service
	WriteProfile(t, sw, &storage.WriteProfileParams{
		Service: service2,
		Type:    profile.TypeCPU,
		Labels:  profile.Labels{{"key1", "val1"}},
	}, "../../../testdata/collector_cpu_3.prof")

	// a profile of different type
	WriteProfile(t, sw, &storage.WriteProfileParams{
		Service: service1,
		Type:    profile.TypeHeap,
		Labels:  profile.Labels{{"key1", "val1"}, {"key2", "val2"}},
	}, "../../../testdata/collector_heap_1.prof")

	// a profile of different labels
	WriteProfile(t, sw, &storage.WriteProfileParams{
		Service: service1,
		Type:    profile.TypeHeap,
		Labels:  profile.Labels{{"key3", "val3"}},
	}, "../../../testdata/collector_heap_2.prof")

	// just some old timestamp to simplify querying
	createdAtMin := time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("by service", func(t *testing.T) {
		params := &storage.FindProfilesParams{
			Service:      service1,
			CreatedAtMin: createdAtMin,
		}
		ids, err := sr.FindProfileIDs(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, ids, 4)
	})

	t.Run("by service-type", func(t *testing.T) {
		params := &storage.FindProfilesParams{
			Service:      service1,
			Type:         profile.TypeCPU,
			CreatedAtMin: createdAtMin,
		}
		ids, err := sr.FindProfileIDs(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, ids, 2)
	})

	t.Run("by service-labels", func(t *testing.T) {
		params := &storage.FindProfilesParams{
			Service:      service1,
			Labels:       profile.Labels{{"key1", "val1"}},
			CreatedAtMin: createdAtMin,
		}
		ids, err := sr.FindProfileIDs(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, ids, 3)
	})

	t.Run("by service-type-labels", func(t *testing.T) {
		params := &storage.FindProfilesParams{
			Service:      service1,
			Type:         profile.TypeHeap,
			Labels:       profile.Labels{{"key2", "val2"}},
			CreatedAtMin: createdAtMin,
		}
		ids, err := sr.FindProfileIDs(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, ids, 1)
	})

	t.Run("with limit", func(t *testing.T) {
		params := &storage.FindProfilesParams{
			Service:      service1,
			CreatedAtMin: createdAtMin,
			Limit:        2,
		}
		ids, err := sr.FindProfileIDs(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, ids, 2)
	})

	t.Run("with time window", func(t *testing.T) {
		service := genServiceName()
		createdAt := time.Now().UTC().Truncate(time.Second)

		// store 4 new profiles created at t-1h, t, t+1m, t+1h
		WriteProfile(t, sw, &storage.WriteProfileParams{
			Service:   service,
			Type:      profile.TypeCPU,
			CreatedAt: createdAt.Add(-time.Hour),
		}, "../../../testdata/collector_cpu_1.prof")

		WriteProfile(t, sw, &storage.WriteProfileParams{
			Service:   service,
			Type:      profile.TypeCPU,
			CreatedAt: createdAt,
		}, "../../../testdata/collector_cpu_1.prof")

		WriteProfile(t, sw, &storage.WriteProfileParams{
			Service:   service,
			Type:      profile.TypeCPU,
			CreatedAt: createdAt.Add(time.Minute).Add(-1 * time.Second),
		}, "../../../testdata/collector_cpu_1.prof")

		WriteProfile(t, sw, &storage.WriteProfileParams{
			Service:   service,
			Type:      profile.TypeCPU,
			CreatedAt: createdAt.Add(time.Hour),
		}, "../../../testdata/collector_cpu_1.prof")

		// searching with [t, t+1m] must filter out results outside of the time window
		params := &storage.FindProfilesParams{
			Service:      service,
			Type:         profile.TypeCPU,
			CreatedAtMin: createdAt,
			CreatedAtMax: createdAt.Add(time.Minute),
		}
		ids, err := sr.FindProfileIDs(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, ids, 2)
	})

	t.Run("nothing found", func(t *testing.T) {
		params := &storage.FindProfilesParams{
			Service:      service1,
			Type:         profile.TypeHeap,
			Labels:       profile.Labels{{"key3", "val1"}},
			CreatedAtMin: createdAtMin,
		}
		_, err := sr.FindProfileIDs(context.Background(), params)
		require.Equal(t, storage.ErrNotFound, err)
	})

	t.Run("no service", func(t *testing.T) {
		params := &storage.FindProfilesParams{
			Type: profile.TypeHeap,
		}
		_, err := sr.FindProfileIDs(context.Background(), params)
		require.Error(t, err)
	})

	t.Run("no createdAtMin", func(t *testing.T) {
		params := &storage.FindProfilesParams{
			Service: service1,
			Type:    profile.TypeHeap,
		}
		_, err := sr.FindProfileIDs(context.Background(), params)
		require.Error(t, err)
	})
}

func testListProfiles(t *testing.T, sr storage.Reader, sw storage.Writer) {
	service1 := genServiceName()

	var (
		pids []profile.ID
		pps  []*pprofProfile.Profile
	)

	for n := 1; n <= 3; n++ {
		fileName := fmt.Sprintf("../../../testdata/collector_cpu_%d.prof", n)
		params := &storage.WriteProfileParams{
			Service: service1,
			Type:    profile.TypeCPU,
			Labels:  profile.Labels{{"key1", "val1"}},
		}
		meta, data := WriteProfile(t, sw, params, fileName)
		pids = append(pids, meta.ProfileID)

		pp, err := pprofProfile.ParseData(data)
		require.NoError(t, err)
		pps = append(pps, pp)
	}

	require.Len(t, pids, 3)

	t.Run("found", func(t *testing.T) {
		list, err := sr.ListProfiles(context.Background(), pids[1:])
		require.NoError(t, err)
		defer list.Close()

		var found int
		for list.Next() {
			ppr, err := list.Profile()
			require.NoError(t, err)

			gotpp, err := pprofProfile.Parse(ppr)
			require.NoError(t, err)

			found++
			assert.True(t, pprofutil.ProfilesEqual(pps[found], gotpp), "profiles %d must be equal", found)
		}
	})

	t.Run("no ids", func(t *testing.T) {
		_, err := sr.ListProfiles(context.Background(), nil)
		require.Error(t, err)
	})

	t.Run("context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		list, err := sr.ListProfiles(ctx, pids[1:])
		require.NoError(t, err)
		defer list.Close()

		assert.True(t, list.Next())

		cancel()

		assert.False(t, list.Next())
		_, err = list.Profile()
		assert.Equal(t, context.Canceled, err)
	})
}

func testListServices(t *testing.T, sr storage.Reader, sw storage.Writer) {
	service1 := genServiceName()
	service2 := genServiceName()

	for n := 1; n <= 2; n++ {
		fileName := fmt.Sprintf("../../../testdata/collector_cpu_%d.prof", n)
		params := &storage.WriteProfileParams{
			Service: service1,
			Type:    profile.TypeCPU,
			Labels:  profile.Labels{{"key1", "val1"}},
		}
		WriteProfile(t, sw, params, fileName)
	}

	// a profile of different service
	WriteProfile(t, sw, &storage.WriteProfileParams{
		Service: service2,
		Type:    profile.TypeCPU,
		Labels:  profile.Labels{{"key1", "val1"}},
	}, "../../../testdata/collector_cpu_3.prof")

	services, err := sr.ListServices(context.Background())
	require.NoError(t, err)

	sset := make(map[string]struct{})
	for _, s := range services {
		_, ok := sset[s]
		assert.False(t, ok, "duplicate service %q in list %v", s, services)
		sset[s] = struct{}{}
	}
}
