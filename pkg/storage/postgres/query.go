package postgres

import (
	"fmt"
	"strings"

	"github.com/lib/pq"
)

const (
	sqlSOIProfileLabels = `
		WITH new_labels (id) AS (
			INSERT INTO pprof_profile_labels (service, instance_id, labels)
			VALUES ($1, $2, $3)
			ON CONFLICT DO NOTHING
			RETURNING id
		)
		SELECT id FROM pprof_profile_labels
		WHERE service = $1 AND instance_id = $2 AND labels = $3
		UNION ALL
		SELECT id FROM new_labels;`

	sqlInsertProfiles = `
		INSERT INTO pprof_profiles (created_at, received_at, type, period, labels_id)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING profile_id;`

	sqlSelectSamplesTmpl = `
		SELECT p.profile_id, p.created_at, s.locations, s.labels, %[2]s
		FROM (
			SELECT p.profile_id, p.created_at
			FROM pprof_profiles p
			INNER JOIN pprof_profile_labels v ON p.labels_id = v.id
			-- where clause placeholder
			%%s
		) p
		INNER JOIN %[1]s s ON p.profile_id = s.profile_id
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
	return samplesQueryBuilder{
		insertQuery: buildInsertSamplesSQL(table, cols...),
		selectQuery: buildSelectSamplesSQL(table, cols...),
	}
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
