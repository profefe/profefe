package storagetest

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/pprofutil"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func RunTestSuite(t *testing.T, st storage.Storage) {
	cases := map[string]func(t *testing.T, st storage.Storage){
		"WriteProfile":   testWriteFindProfile,
		"FindProfileIDs": testFindProfileIDs,
		"ListProfiles":   testListProfiles,
		"ListServices":   testListServices,
	}

	for name, testFunc := range cases {
		t.Run(name, func(t *testing.T) {
			testFunc(t, st)
		})
	}
}

func testWriteFindProfile(t *testing.T, st storage.Storage) {
	service := genServiceName()
	createdAt := time.Now()
	wparams := &storage.WriteProfileParams{
		Service:   service,
		Type:      profile.TypeCPU,
		Labels:    profile.Labels{{"key1", "val1"}},
		CreatedAt: createdAt,
	}
	meta, data := testWriteProfile(t, st, wparams, "../../../testdata/collector_cpu_1.prof")

	wantPP, err := pprofProfile.ParseData(data)
	require.NoError(t, err)

	list, err := st.ListProfiles(context.Background(), []profile.ID{meta.ProfileID})
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, list.Close())
	})

	require.True(t, list.Next())

	ppr, err := list.Profile()
	require.NoError(t, err)

	gotPP, err := pprofProfile.Parse(ppr)
	require.NoError(t, err)
	assert.True(t, pprofutil.ProfilesEqual(wantPP, gotPP))

	require.False(t, list.Next())
}

func testWriteProfile(t *testing.T, sw storage.Writer, params *storage.WriteProfileParams, fileName string) (profile.Meta, []byte) {
	data, err := ioutil.ReadFile(fileName)
	require.NoError(t, err)

	meta, err := sw.WriteProfile(context.Background(), params, bytes.NewReader(data))
	require.NoError(t, err)
	require.NotEmpty(t, meta.ProfileID)
	require.Equal(t, params.Service, meta.Service)
	require.Equal(t, params.Type, meta.Type)
	require.False(t, meta.CreatedAt.IsZero())

	return meta, data
}

func testFindProfileIDs(t *testing.T, st storage.Storage) {
	service1 := genServiceName()
	service2 := genServiceName()

	for n := 1; n <= 2; n++ {
		fileName := fmt.Sprintf("../../../testdata/collector_cpu_%d.prof", n)
		params := &storage.WriteProfileParams{
			Service: service1,
			Type:    profile.TypeCPU,
			Labels:  profile.Labels{{"key1", "val1"}},
		}
		testWriteProfile(t, st, params, fileName)
	}

	// a profile of different service
	testWriteProfile(t, st, &storage.WriteProfileParams{
		Service: service2,
		Type:    profile.TypeCPU,
		Labels:  profile.Labels{{"key1", "val1"}},
	}, "../../../testdata/collector_cpu_3.prof")

	// a profile of different type
	testWriteProfile(t, st, &storage.WriteProfileParams{
		Service: service1,
		Type:    profile.TypeHeap,
		Labels:  profile.Labels{{"key1", "val1"}, {"key2", "val2"}},
	}, "../../../testdata/collector_heap_1.prof")

	// a profile of different labels
	testWriteProfile(t, st, &storage.WriteProfileParams{
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
		ids, err := st.FindProfileIDs(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, ids, 4)
	})

	t.Run("by service-type", func(t *testing.T) {
		params := &storage.FindProfilesParams{
			Service:      service1,
			Type:         profile.TypeCPU,
			CreatedAtMin: createdAtMin,
		}
		ids, err := st.FindProfileIDs(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, ids, 2)
	})

	t.Run("by service-labels", func(t *testing.T) {
		params := &storage.FindProfilesParams{
			Service:      service1,
			Labels:       profile.Labels{{"key1", "val1"}},
			CreatedAtMin: createdAtMin,
		}
		ids, err := st.FindProfileIDs(context.Background(), params)
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
		ids, err := st.FindProfileIDs(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, ids, 1)
	})

	t.Run("with limit", func(t *testing.T) {
		params := &storage.FindProfilesParams{
			Service:      service1,
			CreatedAtMin: createdAtMin,
			Limit:        2,
		}
		ids, err := st.FindProfileIDs(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, ids, 2)
	})

	t.Run("with time window", func(t *testing.T) {
		service := genServiceName()
		createdAt := time.Now().UTC().Truncate(time.Second)

		// store 4 new profiles created at t-1h, t, t+1m, t+1h
		testWriteProfile(t, st, &storage.WriteProfileParams{
			Service:   service,
			Type:      profile.TypeCPU,
			CreatedAt: createdAt.Add(-time.Hour),
		}, "../../../testdata/collector_cpu_1.prof")

		testWriteProfile(t, st, &storage.WriteProfileParams{
			Service:   service,
			Type:      profile.TypeCPU,
			CreatedAt: createdAt,
		}, "../../../testdata/collector_cpu_1.prof")

		testWriteProfile(t, st, &storage.WriteProfileParams{
			Service:   service,
			Type:      profile.TypeCPU,
			CreatedAt: createdAt.Add(time.Minute),
		}, "../../../testdata/collector_cpu_1.prof")

		testWriteProfile(t, st, &storage.WriteProfileParams{
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
		ids, err := st.FindProfileIDs(context.Background(), params)
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
		_, err := st.FindProfileIDs(context.Background(), params)
		require.Equal(t, storage.ErrNotFound, err)
	})

	t.Run("no service", func(t *testing.T) {
		params := &storage.FindProfilesParams{
			Type: profile.TypeHeap,
		}
		_, err := st.FindProfileIDs(context.Background(), params)
		require.Error(t, err)
	})

	t.Run("no createdAtMin", func(t *testing.T) {
		params := &storage.FindProfilesParams{
			Service: service1,
			Type:    profile.TypeHeap,
		}
		_, err := st.FindProfileIDs(context.Background(), params)
		require.Error(t, err)
	})
}

func testListProfiles(t *testing.T, st storage.Storage) {
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
		meta, data := testWriteProfile(t, st, params, fileName)
		pids = append(pids, meta.ProfileID)

		pp, err := pprofProfile.ParseData(data)
		require.NoError(t, err)
		pps = append(pps, pp)
	}

	require.Len(t, pids, 3)

	t.Run("found", func(t *testing.T) {
		list, err := st.ListProfiles(context.Background(), pids[1:])
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
		_, err := st.ListProfiles(context.Background(), nil)
		require.Error(t, err)
	})

	t.Run("context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		list, err := st.ListProfiles(ctx, pids[1:])
		require.NoError(t, err)
		defer list.Close()

		assert.True(t, list.Next())

		cancel()

		assert.False(t, list.Next())
		_, err = list.Profile()
		assert.Equal(t, context.Canceled, err)
	})
}

func testListServices(t *testing.T, st storage.Storage) {
	service1 := genServiceName()
	service2 := genServiceName()

	for n := 1; n <= 2; n++ {
		fileName := fmt.Sprintf("../../../testdata/collector_cpu_%d.prof", n)
		params := &storage.WriteProfileParams{
			Service: service1,
			Type:    profile.TypeCPU,
			Labels:  profile.Labels{{"key1", "val1"}},
		}
		testWriteProfile(t, st, params, fileName)
	}

	// a profile of different service
	testWriteProfile(t, st, &storage.WriteProfileParams{
		Service: service2,
		Type:    profile.TypeCPU,
		Labels:  profile.Labels{{"key1", "val1"}},
	}, "../../../testdata/collector_cpu_3.prof")

	services, err := st.ListServices(context.Background())
	require.NoError(t, err)

	sset := make(map[string]struct{})
	for _, s := range services {
		_, ok := sset[s]
		assert.False(t, ok, "duplicate service %q in list %v", s, services)
		sset[s] = struct{}{}
	}
}

func genServiceName() string {
	return fmt.Sprintf("test-service-%x", time.Now().Nanosecond())
}
