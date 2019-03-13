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
		WITH pprof_samples AS (
			SELECT service_id, created_at, locations, labels, %[2]s
			FROM
			(SELECT sample_id, array_agg(l.location_id) locations, labels, %[3]s
				FROM pprof_samples_tmp tmp
				INNER JOIN pprof_locations l
					ON tmp.func_name = l.func_name AND tmp.file_name = l.file_name AND tmp.line = l.line
				GROUP BY sample_id, labels, %[2]s
			) AS t,

			(SELECT service_id
				FROM services WHERE build_id = $1 AND token = $2) AS v,

			(VALUES ($3::timestamp)) AS d (created_at)
		)
		INSERT INTO %[1]s (service_id, created_at, locations, labels, %[2]s)
		SELECT service_id, created_at, locations, labels, %[4]s
		FROM pprof_samples;`

	sqlSelectLocations = `
		SELECT l.location_id, l.func_name, l.file_name, l.line 
		FROM pprof_locations l
		WHERE l.location_id = ANY($1);`

	sqlSelectSamplesTmpl = `
		SELECT s.service_id, s.created_at, s.locations, s.labels, %[2]s
		FROM %[1]s s
		INNER JOIN services v ON s.service_id = v.service_id
		%%s -- where clause placeholder
		ORDER BY s.created_at;`
)

var sqlCopyTable = pq.CopyIn(
	"pprof_samples_tmp",
	"sample_id",
	"func_name",
	"file_name",
	"line",
	"values_all",
	"labels",
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
	tCols := make([]string, 0, len(cols))
	nullifCols := make([]string, 0, len(cols))
	for n, col := range cols {
		tCols = append(tCols, fmt.Sprintf("values_all[%d] AS %s", n+1, col))
		nullifCols = append(nullifCols, fmt.Sprintf("NULLIF(%s, 0)", col))
	}
	return fmt.Sprintf(
		sqlInsertSamplesTmpl,
		table,
		strings.Join(cols, ","),
		strings.Join(tCols, ","),
		strings.Join(nullifCols, ","),
	)
}

func buildSelectSamplesSQL(table string, cols ...string) string {
	return fmt.Sprintf(sqlSelectSamplesTmpl, table, strings.Join(cols, ","))
}
