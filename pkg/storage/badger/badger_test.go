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
	meta := &profile.ProfileMeta{
		ProfileID:  profile.NewProfileID(),
		Service:    service,
		Type:       profile.CPUProfile,
		InstanceID: iid,
		Labels:     profile.Labels{{"key1", "val2"}},
	}

	data, err := ioutil.ReadFile("../../../testdata/test_cpu1.prof")
	require.NoError(t, err)
	pp, err := pprofProfile.ParseData(data)
	require.NoError(t, err)

	pf := profile.NewProfileFactory(pp)

	err = st.WriteProfile(context.Background(), meta, pf)
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

	iid := profile.NewInstanceID()
	service := fmt.Sprintf("test-service-%s", iid)
	labels := profile.Labels{{"key1", "val1"}}

	for i := 0; i < 3; i++ {
		meta := &profile.ProfileMeta{
			ProfileID:  profile.NewProfileID(),
			Service:    service,
			Type:       profile.CPUProfile,
			InstanceID: iid,
			Labels:     labels,
		}

		fileName := fmt.Sprintf("../../../testdata/collector_cpu_%d.prof", i+1)
		data, err := ioutil.ReadFile(fileName)
		require.NoError(t, err)
		pp, err := pprofProfile.ParseData(data)
		require.NoError(t, err)

		err = st.WriteProfile(context.Background(), meta, profile.NewProfileFactory(pp))
		require.NoError(t, err)
	}

	params := &storage.FindProfilesParams{
		Service:      service,
		CreatedAtMin: time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC), // just some old date
	}
	gotpfs, err := st.FindProfiles(context.Background(), params)
	require.NoError(t, err)
	require.Len(t, gotpfs, 3)

	// make sure storage returns profiles in the correct order
	var prevTime int64
	for _, pf := range gotpfs {
		pp, err := pf.Profile()
		require.NoError(t, err)
		assert.True(t, prevTime < pp.TimeNanos, "create time must be after previous time")
	}
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

	pid := profile.NewProfileID()
	iid := profile.NewInstanceID()
	service := fmt.Sprintf("test-service-%s", iid)
	meta := &profile.ProfileMeta{
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

	pf := profile.NewProfileFactory(pp)

	err = st.WriteProfile(context.Background(), meta, pf)
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

	pid := profile.NewProfileID()

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