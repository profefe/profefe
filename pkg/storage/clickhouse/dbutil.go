package clickhouse

import (
	"database/sql"
	"fmt"
	"net/url"

	"github.com/profefe/profefe/pkg/log"
)

const (
	sqlCreatePprofProfiles = `
		CREATE TABLE IF NOT EXISTS pprof_profiles (
			profile_key FixedString(12),
			profile_type Enum8(
				'cpu' = 1,
				'heap' = 2,
				'block' = 3,
				'mutex' = 4,
				'goroutine' = 5,
				'threadcreate' = 6,
				'other' = 100
			),
			external_id String,
			service_name LowCardinality(String),
			created_at DateTime,
			labels Nested (
				key LowCardinality(String),
				value String
			)
		) engine=Memory`

	sqlCreatePprofSamples = `
		CREATE TABLE IF NOT EXISTS pprof_samples (
			profile_key FixedString(12),
			digest UInt64,
			locations Nested (
				func_name LowCardinality(String),
				file_name LowCardinality(String),
				lineno UInt16
			),
			values Array(UInt64),
			labels Nested (
				key String,
				value String
			)
		) engine=Memory`
)

func SetupDB(logger *log.Logger, dsn string, dropdb bool) (err error) {
	dsnURL, err := url.Parse(dsn)
	if err != nil {
		return err
	}

	database := dsnURL.Query().Get("database")
	if database == "" {
		return fmt.Errorf("no database in clickhouse DNS %q", dsn)
	}

	// remove database path from init-DSN, other the connection will fail if database doesn't exist yet
	q := dsnURL.Query()
	q.Del("database")
	dsnURL.RawQuery = q.Encode()

	initDB, err := sql.Open("clickhouse", dsnURL.String())
	if err != nil {
		return fmt.Errorf("failed to open init db: %w", err)
	}
	defer initDB.Close()

	if dropdb {
		logger.Infof("drop database %q", database)
		if _, err := initDB.Exec(fmt.Sprintf(`DROP DATABASE IF EXISTS %s`, database)); err != nil {
			return err
		}
	}

	if _, err := initDB.Exec(fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, database)); err != nil {
		return err
	}

	return nil
}

func SetupTables(db *sql.DB) error {
	queries := []string{
		sqlCreatePprofProfiles,
		sqlCreatePprofSamples,
	}

	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			return err
		}
	}

	return nil
}
