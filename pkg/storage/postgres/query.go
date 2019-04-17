package postgres

import (
	"fmt"
	"strings"

	"github.com/lib/pq"
)

const (
	sqlInsertService = `
		INSERT INTO services (build_id, token, name, created_at, labels)
		VALUES ($1, $2, $3, $4, $5);`

	sqlSelectServices = `
		SELECT name, created_at, labels FROM services
		ORDER BY created_at;`

	sqlSelectServicesByName = `
		SELECT name, created_at, labels FROM services
		WHERE name = $1
		ORDER BY created_at;`

	sqlInsertProfiles = `
		INSERT INTO pprof_profiles (service_id, created_at, type, period)
		SELECT * FROM
			(SELECT service_id FROM services WHERE build_id = $1 AND token = $2) AS v,
			(VALUES ($3::timestamp, $4::smallint, $5::bigint)) AS d (created_at, type, period)
		RETURNING profile_id;`

	sqlSelectSamplesTmpl = `
		SELECT p.created_at, s.locations, s.labels, %[2]s
		FROM %[1]s s
		INNER JOIN pprof_profiles p ON s.profile_id = p.profile_id
		INNER JOIN services v ON p.service_id = v.service_id

		-- where clause placeholder
		%%s
		ORDER BY p.created_at;`
)

var (
	sqlSamplesCPU = newSamplesQueryBuilder(
		"pprof_samples_cpu",
		"samples_count",
		"cpu_nanos",
	)
	sqlSamplesHeap = newSamplesQueryBuilder(
		"pprof_samples_heap",
		"alloc_objects",
		"alloc_bytes",
		"inuse_objects",
		"inuse_bytes",
	)

	// TODO: more samples sql
	sqlSamplesBlock = newSamplesQueryBuilder(
		"pprof_samples_block",
	)
	sqlSamplesMutex = newSamplesQueryBuilder(
		"pprof_samples_mutex",
	)
)

type samplesQueryBuilder struct {
	insertQuery, selectQuery string
}

func newSamplesQueryBuilder(table string, cols ...string) samplesQueryBuilder {
	var s samplesQueryBuilder
	s.insertQuery = buildInsertSamplesSQL(table, cols...)
	s.selectQuery = buildSelectSamplesSQL(table, cols...)
	return s
}

func (s samplesQueryBuilder) ToInsertSQL() string {
	return s.insertQuery
}

func (s samplesQueryBuilder) ToSelectSQL(whereParts ...string) string {
	var whereClause string
	if len(whereParts) > 0 {
		whereClause = "WHERE " + strings.Join(whereParts, " AND ")
	}
	return fmt.Sprintf(s.selectQuery, whereClause)
}

func buildInsertSamplesSQL(table string, cols ...string) string {
	tCols := append([]string{"profile_id", "locations", "labels"}, cols...)
	return pq.CopyIn(
		table,
		tCols...,
	)
}

func buildSelectSamplesSQL(table string, cols ...string) string {
	return fmt.Sprintf(sqlSelectSamplesTmpl, table, strings.Join(cols, ","))
}
