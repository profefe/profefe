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
	badgerStorage "github.com/profefe/profefe/pkg/storage/badger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestStorage_WriteFind(t *testing.T) {
	st, teardown := setupTestStorage(t)
	defer teardown()

	service := "test-service-1"
	meta := profile.Meta{
		ProfileID: profile.NewID(),
		Service:   service,
		Type:      profile.CPUProfile,
		Labels:    profile.Labels{{"key1", "val1"}},
	}

	data := testWriteProfile(t, st, "../../../testdata/collector_cpu_1.prof", meta)

	pp, err := pprofProfile.ParseData(data)
	require.NoError(t, err)

	params := &storage.FindProfilesParams{
		Service:      service,
		Type:         profile.CPUProfile,
		CreatedAtMin: time.Unix(0, pp.TimeNanos),
	}
	found, err := st.FindProfiles(context.Background(), params)
	require.NoError(t, err)
	require.Len(t, found, 1)

	assert.Equal(t, service, found[0].Service)
	assert.Equal(t, profile.CPUProfile, found[0].Type)

	list, err := st.ListProfiles(context.Background(), []profile.ID{found[0].ProfileID})
	require.NoError(t, err)

	require.True(t, list.Next())
	gotpp, err := list.Profile()
	require.NoError(t, err)

	assert.True(t, pprofutil.ProfilesEqual(pp, gotpp))

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
		meta := profile.Meta{
			ProfileID: profile.NewID(),
			Service:   service1,
			Type:      profile.CPUProfile,
			Labels:    profile.Labels{{"key1", "val1"}},
		}
		testWriteProfile(t, st, fileName, meta)
	}

	// a profile of different service
	testWriteProfile(
		t,
		st,
		"../../../testdata/collector_cpu_3.prof",
		profile.Meta{
			ProfileID: profile.NewID(),
			Service:   service2,
			Type:      profile.CPUProfile,
			Labels:    profile.Labels{{"key1", "val1"}},
		},
	)

	// a profile of different type
	testWriteProfile(
		t,
		st,
		"../../../testdata/collector_heap_1.prof",
		profile.Meta{
			ProfileID: profile.NewID(),
			Service:   service1,
			Type:      profile.HeapProfile,
			Labels:    profile.Labels{{"key1", "val1"}, {"key2", "val2"}},
		},
	)

	// a profile of different labels
	testWriteProfile(
		t,
		st,
		"../../../testdata/collector_heap_2.prof",
		profile.Meta{
			ProfileID: profile.NewID(),
			Service:   service1,
			Type:      profile.HeapProfile,
			Labels:    profile.Labels{{"key3", "val3"}},
		},
	)

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
			Type:         profile.CPUProfile,
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
			Type:         profile.HeapProfile,
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
			Type:         profile.HeapProfile,
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
		pid := profile.NewID()
		pids = append(pids, pid)
		fileName := fmt.Sprintf("../../../testdata/collector_cpu_%d.prof", n)
		meta := profile.Meta{
			ProfileID: pid,
			Service:   service1,
			Type:      profile.CPUProfile,
			Labels:    profile.Labels{{"key1", "val1"}},
		}
		data := testWriteProfile(t, st, fileName, meta)

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
			gotpp, err := list.Profile()
			require.NoError(t, err)

			found++
			assert.True(t, pprofutil.ProfilesEqual(pps[found], gotpp), "profiles %d must be equal", found)
		}
	})

	t.Run("no ids", func(t *testing.T) {
		_, err := st.ListProfiles(context.Background(), nil)
		require.Error(t, err)
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
		meta := profile.Meta{
			ProfileID: profile.NewID(),
			Service:   service1,
			Type:      profile.CPUProfile,
			Labels:    profile.Labels{{"key1", "val1"}},
		}
		testWriteProfile(t, st, fileName, meta)
	}

	// a profile of different service
	testWriteProfile(
		t,
		st,
		"../../../testdata/collector_cpu_3.prof",
		profile.Meta{
			ProfileID: profile.NewID(),
			Service:   service2,
			Type:      profile.CPUProfile,
			Labels:    profile.Labels{{"key1", "val1"}},
		},
	)

	services, err := st.ListServices(context.Background())
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{service1, service2}, services)
}

func testWriteProfile(t testing.TB, st *badgerStorage.Storage, fileName string, meta profile.Meta) []byte {
	data, err := ioutil.ReadFile(fileName)
	require.NoError(t, err)

	err = st.WriteProfile(context.Background(), meta, bytes.NewReader(data))
	require.NoError(t, err)

	return data
}

func setupTestStorage(t testing.TB) (st *badgerStorage.Storage, teardown func()) {
	dbPath, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)

	opts := badger.DefaultOptions(dbPath)
	db, err := badger.Open(opts)
	require.NoError(t, err)

	testLogger := zaptest.NewLogger(t)
	st = badgerStorage.New(log.New(testLogger), db, 0)

	teardown = func() {
		db.Close()
		os.RemoveAll(dbPath)
	}

	return st, teardown
}
