package clickhouse_test

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/profefe/profefe/pkg/log"
	storageCH "github.com/profefe/profefe/pkg/storage/clickhouse"
	"github.com/profefe/profefe/pkg/storage/storagetest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

func TestStorage(t *testing.T) {
	chDSN := os.Getenv("CLICKHOUSE_DSN")
	if chDSN == "" {
		t.SkipNow()
	}

	logger := log.New(zaptest.NewLogger(t, zaptest.Level(zapcore.FatalLevel)))

	db := setupDB(t, logger, chDSN)
	defer db.Close()

	profilesWriter := storageCH.NewProfilesWriter(logger, db)
	samplesWriter := storageCH.NewSamplesWriter(logger, db)

	st, err := storageCH.NewStorage(logger, db, profilesWriter, samplesWriter)
	require.NoError(t, err)

	// only subset of the test suite is supported
	t.Run("TestFindProfileIDs", func(t *testing.T) {
		storagetest.TestFindProfileIDs(t, st)
	})
	t.Run("TestListServices", func(t *testing.T) {
		storagetest.TestListServices(t, st)
	})
}

func setupDB(t *testing.T, logger *log.Logger, dsn string) *sql.DB {
	err := storageCH.SetupDB(logger, dsn, true)
	require.NoError(t, err)

	db, err := sql.Open("clickhouse", dsn)
	require.NoError(t, err)

	require.NoError(t, db.Ping())
	require.NoError(t, storageCH.SetupTables(db))

	return db
}
