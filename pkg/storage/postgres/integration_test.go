// +build integration

package postgres_test

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	stdlog "log"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"

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

func TestPqStorage(t *testing.T) {
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
		InstanceID: iid,
		Labels:     profile.Labels{{"key1", "val2"}},
	}

	data, err := ioutil.ReadFile("../../../testdata/test_cpu1.prof")
	require.NoError(t, err)
	pp, err := pprofProfile.ParseData(data)
	require.NoError(t, err)

	err = st.CreateProfile(ctx, profile.CPUProfile, meta, pp)
	require.NoError(t, err)

	filter := &profile.GetProfileFilter{
		Service:      service,
		Type:         profile.CPUProfile,
		CreatedAtMin: now,
	}
	gotpp, err := st.GetProfile(ctx, filter)
	require.NoError(t, err)

	assert.Equal(t, pp.PeriodType, gotpp.PeriodType)

	want := getProfileString(t, pp)
	got := getProfileString(t, gotpp)
	//t.Logf("got\n%s\n===\nwant\n%s\n", want, got)
	assert.Equal(t, want, got)
}

// returns a string representation of a profile that we can use to check profiles are equal
func getProfileString(t *testing.T, pp *pprofProfile.Profile) string {
	f, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	pp.Compact().WriteUncompressed(f)

	var buf bytes.Buffer
	cmd := exec.Command("go", "tool", "pprof", "-top", f.Name())
	cmd.Stdout = &buf
	require.NoError(t, cmd.Run())

	s := buf.String()
	// strip profile header
	if n := strings.Index(s, "Showing nodes"); n > 0 {
		return s[n:]
	}
	return s
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
