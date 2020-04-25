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
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

func TestStorage(t *testing.T) {
	chDSN := os.Getenv("CLICKHOUSE_DSN")
	if chDSN == "" {
		t.SkipNow()
	}

	db := setupDB(t, chDSN)
	defer db.Close()

	logger := log.New(zaptest.NewLogger(t, zaptest.Level(zapcore.FatalLevel)))

	profilesWriter := storageCH.NewProfilesWriter(logger, db)
	samplesWriter := storageCH.NewSamplesWriter(logger, db)

	st, err := storageCH.NewStorage(logger, db, profilesWriter, samplesWriter)
	require.NoError(t, err)

	t.Run("Reader", func(t *testing.T) {
		ts := &ReaderTestSuite{
			DB: db,

			ReaderTestSuite: &storagetest.ReaderTestSuite{
				Reader: st,
				Writer: st,
			},
		}
		suite.Run(t, ts)
	})
}

type ReaderTestSuite struct {
	*storagetest.ReaderTestSuite

	DB *sql.DB
}

func (ts *ReaderTestSuite) SetupTest() {
	_, err := ts.DB.Exec(`TRUNCATE TABLE pprof_profiles`)
	ts.Require().NoError(err)

	_, err = ts.DB.Exec(`TRUNCATE TABLE pprof_samples`)
	ts.Require().NoError(err)
}

// not supported by storage
func (ts *ReaderTestSuite) TestListProfiles() {
	ts.T().SkipNow()
}

func setupDB(t *testing.T, dsn string) *sql.DB {
	db, err := sql.Open("clickhouse", dsn)
	require.NoError(t, err)

	require.NoError(t, db.Ping())

	return db
}
