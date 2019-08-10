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

func TestStorage_FindWriteProfile(t *testing.T) {
	st, teardown := setupTestStorage(t)
	defer teardown()

	pid := profile.NewProfileID()
	iid := profile.NewInstanceID()
	service := fmt.Sprintf("test-service-%s", iid)
	meta := &profile.ProfileMeta{
		ProfileID:  pid,
		Service:    service,
		InstanceID: iid,
		Labels:     profile.Labels{{"key1", "val2"}},
	}

	data, err := ioutil.ReadFile("../../../testdata/test_cpu1.prof")
	require.NoError(t, err)
	pp, err := pprofProfile.ParseData(data)
	require.NoError(t, err)

	pf := profile.NewProfileFactory(pp)

	err = st.WriteProfile(context.Background(), profile.CPUProfile, meta, pf)
	require.NoError(t, err)

	req := &storage.FindProfileRequest{
		Service:      service,
		Type:         profile.CPUProfile,
		CreatedAtMin: time.Unix(0, pp.TimeNanos),
	}
	gotpf, err := st.FindProfile(context.Background(), req)
	require.NoError(t, err)

	gotpp, err := gotpf.Profile()
	require.NoError(t, err)

	assert.True(t, pprofutil.ProfilesEqual(pp, gotpp))
}

func TestStorage_FindProfile_NotFound(t *testing.T) {
	st, teardown := setupTestStorage(t)
	defer teardown()

	req := &storage.FindProfileRequest{
		Service:      "test-service",
		Type:         profile.CPUProfile,
		CreatedAtMin: time.Now().UTC(),
	}
	_, err := st.FindProfile(context.Background(), req)
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
		InstanceID: iid,
		Labels:     profile.Labels{{"key1", "val2"}},
	}

	data, err := ioutil.ReadFile("../../../testdata/test_cpu1.prof")
	require.NoError(t, err)
	pp, err := pprofProfile.ParseData(data)
	require.NoError(t, err)

	pf := profile.NewProfileFactory(pp)

	err = st.WriteProfile(context.Background(), profile.CPUProfile, meta, pf)
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

func setupTestStorage(t *testing.T) (st storage.Storage, teardown func()) {
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
