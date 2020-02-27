package badger_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/pprofutil"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
	storageBadger "github.com/profefe/profefe/pkg/storage/badger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestStorage_WriteFindProfile(t *testing.T) {
	st, teardown := setupTestStorage(t)
	defer teardown()

	service := "test-service-1"
	createdAt := time.Now()
	wparams := &storage.WriteProfileParams{
		Service:   service,
		Type:      profile.TypeCPU,
		Labels:    profile.Labels{{"key1", "val1"}},
		CreatedAt: createdAt,
	}
	meta, data := testWriteProfile(t, st, wparams, "../../../testdata/collector_cpu_1.prof")

	pp, err := pprofProfile.ParseData(data)
	require.NoError(t, err)

	testStorageFindProfileByID(t, pp, st, meta.ProfileID)

	rparams := &storage.FindProfilesParams{
		Service:      service,
		Type:         profile.TypeCPU,
		CreatedAtMin: createdAt,
	}
	found, err := st.FindProfiles(context.Background(), rparams)
	require.NoError(t, err)
	require.Len(t, found, 1)

	assert.Equal(t, service, found[0].Service)
	assert.Equal(t, profile.TypeCPU, found[0].Type)

	testStorageFindProfileByID(t, pp, st, found[0].ProfileID)
}

func testStorageFindProfileByID(t *testing.T, wantpp *pprofProfile.Profile, st *storageBadger.Storage, pid profile.ID) {
	list, err := st.ListProfiles(context.Background(), []profile.ID{pid})
	require.NoError(t, err)

	require.True(t, list.Next())

	ppr, err := list.Profile()
	require.NoError(t, err)

	gotpp, err := pprofProfile.Parse(ppr)
	require.NoError(t, err)
	assert.True(t, pprofutil.ProfilesEqual(wantpp, gotpp))

	require.False(t, list.Next())

	assert.NoError(t, list.Close())
}

func TestStorage_FindProfileIDs_Indexes(t *testing.T) {
	st, teardown := setupTestStorage(t)
	defer teardown()

	service1 := "test-service-1"
	service2 := "test-service-2"

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

	// just some old date to simplify querying
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

	t.Run("by service type", func(t *testing.T) {
		params := &storage.FindProfilesParams{
			Service:      service1,
			Type:         profile.TypeCPU,
			CreatedAtMin: createdAtMin,
		}
		ids, err := st.FindProfileIDs(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, ids, 2)
	})

	t.Run("by service labels", func(t *testing.T) {
		params := &storage.FindProfilesParams{
			Service:      service1,
			Labels:       profile.Labels{{"key1", "val1"}},
			CreatedAtMin: createdAtMin,
		}
		ids, err := st.FindProfileIDs(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, ids, 3)
	})

	t.Run("by service labels type", func(t *testing.T) {
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
}

func TestStorage_ListProfiles_MultipleResults(t *testing.T) {
	st, teardown := setupTestStorage(t)
	defer teardown()

	service1 := "test-service-1"

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

	assert.Len(t, pids, 3)

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

	t.Run("context cancel", func(t *testing.T) {
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

func TestStorage_ListServices(t *testing.T) {
	st, teardown := setupTestStorage(t)
	defer teardown()

	_, err := st.ListServices(context.Background())
	require.Equal(t, storage.ErrNotFound, err)

	service1 := "test-service-1"
	service2 := "test-service-2"

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
	assert.ElementsMatch(t, []string{service1, service2}, services)
}

func testWriteProfile(t testing.TB, st storage.Writer, params *storage.WriteProfileParams, fileName string) (profile.Meta, []byte) {
	data, err := ioutil.ReadFile(fileName)
	require.NoError(t, err)

	meta, err := st.WriteProfile(context.Background(), params, bytes.NewReader(data))
	require.NoError(t, err)
	require.NotEmpty(t, meta.ProfileID)
	require.Equal(t, meta.Service, params.Service)
	require.Equal(t, meta.Type, params.Type)
	require.Equal(t, meta.CreatedAt, params.CreatedAt)

	return meta, data
}

func setupTestStorage(t testing.TB) (st *storageBadger.Storage, teardown func()) {
	dbPath, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)

	opts := badger.DefaultOptions(dbPath)
	db, err := badger.Open(opts)
	require.NoError(t, err)

	testLogger := zaptest.NewLogger(t)
	st = storageBadger.New(log.New(testLogger), db, 0)

	teardown = func() {
		db.Close()
		os.RemoveAll(dbPath)
	}

	return st, teardown
}
