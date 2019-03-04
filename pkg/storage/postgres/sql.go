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
		CREATE TEMPORARY TABLE IF NOT EXISTS pprof_samples_tmp (sample_id INT, func_name TEXT, file_name TEXT, line INT, values_all BIGINT[]) 
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
		INSERT INTO %[1]s (build_id, token, created_at, received_at, locations, %[2]s)
		SELECT s.build_id, s.token, s.created_at, s.received_at, locations, %[2]s
		FROM (values ($1, $2, $3::timestamp, $4::timestamp)) AS s (build_id, token, created_at, received_at),
	  	(
			SELECT sample_id, array_agg(l.location_id) as locations, %[3]s
			FROM pprof_samples_tmp tmp
			INNER JOIN pprof_locations l 
			ON tmp.func_name = l.func_name AND tmp.file_name = l.file_name AND tmp.line = l.line
			GROUP BY sample_id, %[2]s
		) AS t;`

	sqlSelectLocations = `
		SELECT l.location_id, l.func_name, l.file_name, l.line FROM pprof_locations l
		WHERE l.location_id = any($1);`

	sqlSelectSamplesTmpl = `
		SELECT %[2]s, s.locations FROM %[1]s s, services v
		WHERE s.build_id = v.build_id AND s.token = v.token and v.name = $1 
		%%[1]s
		AND s.created_at BETWEEN $2 AND $3
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

func (s sqlSamplesBuilder) InsertQuery() string {
	return s.insertQuery
}

func (s sqlSamplesBuilder) SelectQuery() string {
	return s.selectQuery
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
