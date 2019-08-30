package badger_test

import (
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

	iid := profile.NewInstanceID()
	service := fmt.Sprintf("test-service-%s", iid)
	meta := &profile.Meta{
		ProfileID:  profile.NewID(),
		Service:    service,
		Type:       profile.CPUProfile,
		InstanceID: iid,
		Labels:     profile.Labels{{"key1", "val2"}},
	}

	data, err := ioutil.ReadFile("../../../testdata/test_cpu1.prof")
	require.NoError(t, err)
	pp, err := pprofProfile.ParseData(data)
	require.NoError(t, err)

	err = st.WriteProfile(context.Background(), meta, profile.NewSingleProfileReader(pp))
	require.NoError(t, err)

	params := &storage.FindProfilesParams{
		Service:      service,
		Type:         profile.CPUProfile,
		CreatedAtMin: time.Unix(0, pp.TimeNanos),
	}
	gotpfs, err := st.FindProfiles(context.Background(), params)
	require.NoError(t, err)

	require.Len(t, gotpfs, 1)

	gotpp, err := gotpfs[0].Profile()
	require.NoError(t, err)

	assert.True(t, pprofutil.ProfilesEqual(pp, gotpp))
}

func TestStorage_FindProfiles_MultipleResults(t *testing.T) {
	st, teardown := setupTestStorage(t)
	defer teardown()

	writeProfile := func(fileName string, meta *profile.Meta) {
		data, err := ioutil.ReadFile(fileName)
		require.NoError(t, err)
		pp, err := pprofProfile.ParseData(data)
		require.NoError(t, err)

		err = st.WriteProfile(context.Background(), meta, profile.NewSingleProfileReader(pp))
		require.NoError(t, err)
	}

	iid := profile.NewInstanceID()
	service := fmt.Sprintf("test-service-%s", iid)

	writeProfile(
		"../../../testdata/test_cpu1.prof",
		&profile.Meta{
			ProfileID:  profile.NewID(),
			Service:    service,
			Type:       profile.CPUProfile,
			InstanceID: iid,
			Labels:     profile.Labels{{"key1", "val1"}},
		},
	)

	writeProfile(
		"../../../testdata/test_heap1.prof",
		&profile.Meta{
			ProfileID:  profile.NewID(),
			Service:    service,
			Type:       profile.HeapProfile,
			InstanceID: iid,
			Labels:     profile.Labels{{"key1", "val1"}},
		},
	)

	for i := 0; i < 3; i++ {
		fileName := fmt.Sprintf("../../../testdata/collector_cpu_%d.prof", i+1)
		writeProfile(
			fileName,
			&profile.Meta{
				ProfileID:  profile.NewID(),
				Service:    service,
				Type:       profile.CPUProfile,
				InstanceID: iid,
				Labels:     profile.Labels{{"key2", "val2"}},
			},
		)
	}

	// just some old date
	createdAtMin := time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("by service and labels", func(t *testing.T) {
		params := &storage.FindProfilesParams{
			Service:      service,
			Labels:       profile.Labels{{"key2", "val2"}},
			CreatedAtMin: createdAtMin,
		}
		gotpfs, err := st.FindProfiles(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, gotpfs, 3)

		// check that storage returns profiles in the correct order
		var prevTime int64
		for _, pf := range gotpfs {
			pp, err := pf.Profile()
			require.NoError(t, err)
			assert.True(t, prevTime < pp.TimeNanos, "create time must be after previous time")
		}
	})

	t.Run("by service and type", func(t *testing.T) {
		params := &storage.FindProfilesParams{
			Service:      service,
			Type:         profile.HeapProfile,
			CreatedAtMin: createdAtMin,
		}
		gotpfs, err := st.FindProfiles(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, gotpfs, 1)
	})

	t.Run("by service labels and type", func(t *testing.T) {
		params := &storage.FindProfilesParams{
			Service:      service,
			Type:         profile.HeapProfile,
			Labels:       profile.Labels{{"key1", "val1"}},
			CreatedAtMin: createdAtMin,
		}
		gotpfs, err := st.FindProfiles(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, gotpfs, 1)
	})

	t.Run("by service", func(t *testing.T) {
		params := &storage.FindProfilesParams{
			Service:      service,
			CreatedAtMin: createdAtMin,
		}
		gotpfs, err := st.FindProfiles(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, gotpfs, 5)
	})
}

func TestStorage_ListProfiles_MultipleResults(t *testing.T) {
	st, teardown := setupTestStorage(t)
	defer teardown()

	writeProfile := func(fileName string, meta *profile.Meta) {
		data, err := ioutil.ReadFile(fileName)
		require.NoError(t, err)
		pp, err := pprofProfile.ParseData(data)
		require.NoError(t, err)

		err = st.WriteProfile(context.Background(), meta, profile.NewSingleProfileReader(pp))
		require.NoError(t, err)
	}

	iid := profile.NewInstanceID()
	service := fmt.Sprintf("test-service-%s", iid)

	writeProfile(
		"../../../testdata/test_cpu1.prof",
		&profile.Meta{
			ProfileID:  profile.NewID(),
			Service:    service,
			Type:       profile.CPUProfile,
			InstanceID: iid,
			Labels:     profile.Labels{{"key1", "val1"}},
		},
	)

	writeProfile(
		"../../../testdata/test_heap1.prof",
		&profile.Meta{
			ProfileID:  profile.NewID(),
			Service:    service,
			Type:       profile.HeapProfile,
			InstanceID: iid,
			Labels:     profile.Labels{{"key1", "val1"}},
		},
	)

	for i := 0; i < 3; i++ {
		fileName := fmt.Sprintf("../../../testdata/collector_cpu_%d.prof", i+1)
		writeProfile(
			fileName,
			&profile.Meta{
				ProfileID:  profile.NewID(),
				Service:    service,
				Type:       profile.CPUProfile,
				InstanceID: iid,
				Labels:     profile.Labels{{"key2", "val2"}},
			},
		)
	}

	// just some old date
	createdAtMin := time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("by service and labels", func(t *testing.T) {
		params := &storage.FindProfilesParams{
			Service:      service,
			Labels:       profile.Labels{{"key2", "val2"}},
			CreatedAtMin: createdAtMin,
		}
		pl, err := st.ListProfiles(context.Background(), params)
		require.NoError(t, err)

		var foundPfs int
		// check that storage returns profiles in the correct order
		var prevTime int64

		for pl.Next() {
			foundPfs++

			pp, err := pl.Reader().Profile()
			require.NoError(t, err)
			assert.True(t, prevTime < pp.TimeNanos, "create time must be after previous time")
		}
		require.NoError(t, pl.Err())
		require.Equal(t, 3, foundPfs)

		require.NoError(t, pl.Close())
	})
}

func TestStorage_FindProfiles_NotFound(t *testing.T) {
	st, teardown := setupTestStorage(t)
	defer teardown()

	params := &storage.FindProfilesParams{
		Service:      "test-service",
		Type:         profile.CPUProfile,
		CreatedAtMin: time.Now().UTC(),
	}
	_, err := st.FindProfiles(context.Background(), params)
	require.Equal(t, storage.ErrNotFound, err)
}

func TestStorage_GetProfile(t *testing.T) {
	st, teardown := setupTestStorage(t)
	defer teardown()

	pid := profile.NewID()
	iid := profile.NewInstanceID()
	service := fmt.Sprintf("test-service-%s", iid)
	meta := &profile.Meta{
		ProfileID:  pid,
		Service:    service,
		Type:       profile.CPUProfile,
		InstanceID: iid,
		Labels:     profile.Labels{{"key1", "val2"}},
	}

	data, err := ioutil.ReadFile("../../../testdata/test_cpu1.prof")
	require.NoError(t, err)
	pp, err := pprofProfile.ParseData(data)
	require.NoError(t, err)

	err = st.WriteProfile(context.Background(), meta, profile.NewSingleProfileReader(pp))
	require.NoError(t, err)

	gotpf, err := st.GetProfile(context.Background(), pid)
	require.NoError(t, err)

	gotpp, err := gotpf.Profile()
	require.NoError(t, err)

	assert.True(t, pprofutil.ProfilesEqual(pp, gotpp))
}

func TestStorage_GetProfile_NotFound(t *testing.T) {
	st, teardown := setupTestStorage(t)
	defer teardown()

	pid := profile.NewID()

	_, err := st.GetProfile(context.Background(), pid)
	require.Equal(t, storage.ErrNotFound, err)
}

func setupTestStorage(t *testing.T) (st *badgerStorage.Storage, teardown func()) {
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
