package badger_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/dgraph-io/badger"
	"github.com/profefe/profefe/pkg/log"
	storageBadger "github.com/profefe/profefe/pkg/storage/badger"
	"github.com/profefe/profefe/pkg/storage/storagetest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

func TestStorage(t *testing.T) {
	dbPath, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)

	opts := badger.DefaultOptions(dbPath)
	db, err := badger.Open(opts)
	require.NoError(t, err)

	t.Cleanup(func() {
		db.Close()
		os.RemoveAll(dbPath)
	})

	testLogger := zaptest.NewLogger(t, zaptest.Level(zapcore.FatalLevel))
	st := storageBadger.NewStorage(log.New(testLogger), db, 0)

	storagetest.RunTestSuite(t, st)
}
