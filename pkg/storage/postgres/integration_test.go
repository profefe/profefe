// +build integration

package postgres_test

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	stdlog "log"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/profefe/profefe/pkg/pprofutil"
	"github.com/profefe/profefe/pkg/storage"

	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

type testSuite struct {
	dsn string
	db  *sql.DB
}

func (ts *testSuite) registerFlags(f *flag.FlagSet) {
	f.StringVar(&ts.dsn, "test.pg.dsn", "postgres:///testdb", "test postgres dsn")
}

var ts testSuite

func TestMain(m *testing.M) {
	os.Exit(runTestMain(m))
}

func runTestMain(m *testing.M) int {
	ts.registerFlags(flag.CommandLine)

	flag.Parse()

	db, err := sql.Open("postgres", ts.dsn)
	if err != nil {
		stdlog.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		stdlog.Fatalf("unable to ping database: %v", err)
	}

	ts.db = db

	return m.Run()
}

func TestStorage(t *testing.T) {
	TruncateDB(t, ts.db)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	testLogger := zaptest.NewLogger(t)
	st, _ := postgres.New(log.New(testLogger), ts.db)

	now := time.Now()
	iid := profile.NewInstanceID()
	service := fmt.Sprintf("test-service-%s", iid)
	meta := &profile.ProfileMeta{
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

	err = st.WriteProfile(ctx, meta, pf)
	require.NoError(t, err)

	req := &storage.FindProfilesParams{
		Service:      service,
		Type:         profile.CPUProfile,
		CreatedAtMin: now,
	}
	gotpf, err := st.FindProfile(ctx, req)
	require.NoError(t, err)

	gotpp, err := gotpf.Profile()
	require.NoError(t, err)

	assert.Equal(t, pp.PeriodType, gotpp.PeriodType)
	assert.True(t, pprofutil.ProfilesEqual(pp, gotpp))
}

func TruncateDB(tb testing.TB, db *sql.DB) {
	tb.Helper()

	rows, err := db.Query(`
		SELECT tablename FROM pg_tables 
		WHERE schemaname = 'public' AND tablename LIKE 'pprof_%';`)
	require.NoError(tb, err)

	var tables []string
	for rows.Next() {
		var table string
		rows.Scan(&table)
		tables = append(tables, table)
	}
	require.NoError(tb, rows.Err())

	query := fmt.Sprintf(`TRUNCATE TABLE %s RESTART IDENTITY`, strings.Join(tables, ","))
	_, err = db.Exec(query)
	require.NoError(tb, err)
}
