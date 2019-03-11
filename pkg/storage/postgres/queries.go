package postgres

import (
	"fmt"
	"strings"

	"github.com/lib/pq"
)

const (
	sqlInsertService = `
		INSERT INTO services (build_id, token, name, created_at, labels)
		VALUES ($1, $2, $3, $4, $5)`

	sqlCreateTempTable = `
		CREATE TEMPORARY TABLE IF NOT EXISTS pprof_samples_tmp (
			sample_id INT,
			func_name TEXT,
			file_name TEXT,
			line INT,
			values_all BIGINT[],
			labels jsonb
		)
		ON COMMIT DELETE ROWS;`

	sqlInsertLocations = `
		INSERT INTO pprof_locations (func_name, file_name, line)
		SELECT tmp.func_name, tmp.file_name, tmp.line
		FROM pprof_samples_tmp tmp 
		LEFT JOIN pprof_locations l
		ON tmp.func_name = l.func_name AND tmp.file_name = l.file_name AND tmp.line = l.line
		WHERE l.func_name IS NULL
		ON CONFLICT (func_name, file_name, line) DO NOTHING;`

	sqlInsertSamplesTmpl = `
		INSERT INTO %[1]s (service, created_at, locations, labels, %[2]s)
		SELECT v.service_id, s.created_at, locations, labels, %[2]s
		FROM (values ($3::timestamp)) AS s (created_at),
		(SELECT service_id FROM services v WHERE build_id = $1 AND token = $2) AS v,
	  	(
			SELECT sample_id, array_agg(l.location_id) as locations, labels, %[3]s
			FROM pprof_samples_tmp tmp
			INNER JOIN pprof_locations l
			ON tmp.func_name = l.func_name AND tmp.file_name = l.file_name AND tmp.line = l.line
			GROUP BY sample_id, labels, %[2]s
		) AS t`

	sqlSelectLocations = `
		SELECT l.location_id, l.func_name, l.file_name, l.line 
		FROM pprof_locations l
		WHERE l.location_id = any($1);`

	sqlSelectSamplesTmpl = `
		SELECT s.service, s.created_at, s.locations, s.labels, %[2]s 
		FROM %[1]s s, services v
		WHERE 
			s.service = v.service_id AND 
			v.name = $1 AND 
			s.created_at BETWEEN $2 AND $3
			%%[1]s
		ORDER BY s.created_at;`
)

var (
	sqlCopyTable = pq.CopyIn(
		"pprof_samples_tmp",
		"sample_id",
		"func_name",
		"file_name",
		"line",
		"values_all",
		"labels",
	)
	sqlSamplesCPU = newSQLSamplesBuilder(
		"pprof_samples_cpu",
		"samples_count",
		"cpu_nanos",
	)
	sqlSamplesHeap = newSQLSamplesBuilder(
		"pprof_samples_heap",
		"alloc_objects",
		"alloc_bytes",
		"inuse_objects",
		"inuse_bytes",
	)

	// TODO: more samples sql
	sqlSamplesBlock = newSQLSamplesBuilder(
		"pprof_samples_block",
	)
	sqlSamplesMutex = newSQLSamplesBuilder(
		"pprof_samples_mutex",
	)
)

type sqlSamplesBuilder struct {
	insertQuery, selectQuery string
}

func newSQLSamplesBuilder(table string, cols ...string) sqlSamplesBuilder {
	var s sqlSamplesBuilder
	s.insertQuery = createInsertSamples(table, cols...)
	s.selectQuery = createSelectSamples(table, cols...)
	return s
}

func (s sqlSamplesBuilder) BuildInsertQuery() string {
	return s.insertQuery
}

func (s sqlSamplesBuilder) BuildSelectQuery(whereParts ...string) string {
	var wp string
	if len(whereParts) > 0 {
		wp = "AND " + strings.Join(whereParts, " AND ")
	}
	return fmt.Sprintf(s.selectQuery, wp)
}

func createInsertSamples(table string, cols ...string) string {
	tCols := make([]string, 0, len(cols))
	for n, col := range cols {
		tCols = append(tCols, fmt.Sprintf("values_all[%d] AS %s", n+1, col))
	}
	return fmt.Sprintf(sqlInsertSamplesTmpl, table, strings.Join(cols, ","), strings.Join(tCols, ","))
}

func createSelectSamples(table string, cols ...string) string {
	return fmt.Sprintf(sqlSelectSamplesTmpl, table, strings.Join(cols, ","))
}
