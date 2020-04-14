package clickhouse

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/cespare/xxhash/v2"
	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
)

const (
	sqlCreatePprofProfiles = `
		CREATE TABLE IF NOT EXISTS %s.pprof_profiles (
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
		CREATE TABLE IF NOT EXISTS %s.pprof_samples (
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

const (
	sqlInsertPprofProfiles = `
		INSERT INTO %s.pprof_profiles (
			profile_key,
			profile_type,
			external_id,
			service_name,
			created_at,
			labels.key,
			labels.value
		)
		VALUES (?, ?, ?, ?, ?, ?)`

	sqlInsertPprofSamples = `
		INSERT INTO %s.pprof_samples (
			profile_key,
			digest,
			locations.func_name,
			locations.file_name,
			locations.lineno,
			values,
			labels.key,
			labels.value
		) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
)

type Storage struct {
	logger *log.Logger
	db     *sql.DB

	database string
}

var _ storage.Writer = (*Storage)(nil)

func New(logger *log.Logger, db *sql.DB, database string) (*Storage, error) {
	var queries []string

	queries = append(queries, fmt.Sprintf(sqlCreatePprofProfiles, database))
	queries = append(queries, fmt.Sprintf(sqlCreatePprofSamples, database))

	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			return nil, err
		}
	}

	return &Storage{
		logger:   logger,
		db:       db,
		database: database,
	}, nil
}

func (st *Storage) WriteProfile(ctx context.Context, params *storage.WriteProfileParams, r io.Reader) (profile.Meta, error) {
	ptype, err := ToProfileType(params.Type)
	if err != nil {
		return profile.Meta{}, err
	}

	pp, err := pprofProfile.Parse(r)
	if err != nil {
		return profile.Meta{}, fmt.Errorf("could not parse profile: %w", err)
	}

	// TODO(narqo) figure out how to notify agent about this error, so it doesn't retry
	//if len(pp.Sample) == 0 {
	//	return profile.Meta{}, fmt.Errorf("profile is empty")
	//}

	createdAt := params.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Unix(0, pp.TimeNanos)
	}
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	createdAt = createdAt.Truncate(time.Second)
	pk := NewProfileKey(createdAt)

	if err := st.writeProfile(ctx, pk, ptype, createdAt, params, pp); err != nil {
		return profile.Meta{}, fmt.Errorf("could not write profile with pk %v, type %v, service %q: %w", pk, ptype, params.Service, err)
	}

	pid := base64.RawURLEncoding.EncodeToString(pk[:])
	meta := profile.Meta{
		ProfileID:  profile.ID(pid),
		ExternalID: params.ExternalID,
		Service:    params.Service,
		Type:       params.Type,
		Labels:     params.Labels,
		CreatedAt:  createdAt,
	}
	return meta, nil
}

type beginTxer interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

func withinTx(ctx context.Context, txer beginTxer, f func(tx *sql.Tx) error) (err error) {
	tx, err := txer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	return f(tx)
}

func (st *Storage) writeProfile(
	ctx context.Context,
	pk ProfileKey,
	ptype ProfileType,
	createdAt time.Time,
	params *storage.WriteProfileParams,
	pp *pprofProfile.Profile,
) error {
	err := withinTx(ctx, st.db, func(tx *sql.Tx) error {
		return st.insertPprofProfiles(ctx, tx, pk, ptype, createdAt, params)
	})
	if err != nil {
		return err
	}

	err = withinTx(ctx, st.db, func(tx *sql.Tx) error {
		return st.insertPprofSamples(ctx, tx, pk, pp.Sample)
	})
	if err != nil {
		return err
	}

	return nil
}

func (st *Storage) insertPprofProfiles(
	ctx context.Context,
	tx *sql.Tx,
	pk ProfileKey,
	ptype ProfileType,
	createdAt time.Time,
	params *storage.WriteProfileParams,
) error {
	query := fmt.Sprintf(sqlInsertPprofProfiles, st.database)
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}

	ln := len(params.Labels)
	labels := make([]string, ln*2)
	for i, label := range params.Labels {
		labels[i] = label.Key
		labels[i+ln] = label.Value
		i++
	}

	args := []interface{}{
		pk,
		ptype,
		params.ExternalID,
		params.Service,
		clickhouse.DateTime(createdAt),
		clickhouse.Array(labels[:ln]),
		clickhouse.Array(labels[ln:]),
	}

	st.logger.Debugw("insertPprofProfiles: insert profile", log.MultiLine("query", query), "args", args)

	if _, err := stmt.ExecContext(ctx, args...); err != nil {
		return fmt.Errorf("could not insert profile: %w", err)
	}

	return stmt.Close()
}

func (st *Storage) insertPprofSamples(ctx context.Context, tx *sql.Tx, pk ProfileKey, samples []*pprofProfile.Sample) error {
	query := fmt.Sprintf(sqlInsertPprofSamples, st.database)
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}

	args := make([]interface{}, 8) // n is for number of inserted values, see query
	args[0] = pk

	samplesDgstr := samplesDigestPool.Get().(*samplesDigest)
	defer samplesDigestPool.Put(samplesDgstr)

	// locations
	var (
		locs  []string
		lines []uint16

		labelKeys, labelVals []string
	)
	for _, sample := range samples {
		if isEmptySample(sample) {
			continue
		}

		nlocs := len(sample.Location)
		sz := nlocs * 2
		if cap(locs) < sz {
			locs = make([]string, sz, sz*2)
		} else {
			locs = locs[:sz]
		}
		if cap(lines) < nlocs {
			lines = make([]uint16, 0, nlocs)
		}

		args[1] = samplesDgstr.Digest(sample)

		funcs, files, lines := collectLocations(sample, locs, lines)
		args[2] = clickhouse.Array(funcs)
		args[3] = clickhouse.Array(files)
		args[4] = clickhouse.Array(lines)
		args[5] = clickhouse.Array(sample.Value)

		labelKeys, labelVals = collectLabels(sample, labelKeys, labelVals)
		args[6] = clickhouse.Array(labelKeys)
		args[7] = clickhouse.Array(labelVals)

		st.logger.Debugw("insertPprofSamples: insert sample", log.MultiLine("query", query), "args", args)

		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			return fmt.Errorf("could not insert sample: %w", err)
		}
	}

	return stmt.Close()
}

func isEmptySample(s *pprofProfile.Sample) bool {
	for _, v := range s.Value {
		if v != 0 {
			return false
		}
	}
	return true
}

type samplesDigest struct {
	hash *xxhash.Digest
	buf  []byte
}

var samplesDigestPool = sync.Pool{
	New: func() interface{} {
		return &samplesDigest{
			hash: xxhash.New(),
			buf:  make([]byte, 0, 65536), // 64KB
		}
	},
}

func (dgst *samplesDigest) Digest(sample *pprofProfile.Sample) uint64 {
	dgst.hash.Reset()

	// locations
	for _, loc := range sample.Location {
		dgst.buf = strconv.AppendUint(dgst.buf, loc.Address, 16)
		for _, line := range loc.Line {
			dgst.buf = append(dgst.buf, '|')
			dgst.buf = append(dgst.buf, line.Function.Filename...)
			dgst.buf = append(dgst.buf, ':')
			dgst.buf = strconv.AppendInt(dgst.buf, line.Line, 10)
			dgst.buf = append(dgst.buf, line.Function.Name...)
		}
	}
	dgst.hash.Write(dgst.buf)
	dgst.buf = dgst.buf[:0]

	// XXX(narqo) generally a sample has way more locations than labels,
	// thus don't bother reusing labels' buffers
	var labels []string

	// string labels
	if len(sample.Label) > 0 {
		labels = make([]string, 0, len(sample.Label))
		for k, v := range sample.Label {
			labels = append(labels, fmt.Sprintf("%q%q", k, v))
		}
		sort.Strings(labels)
		for _, label := range labels {
			dgst.hash.WriteString(label)
		}
	}

	// num labels
	if len(sample.NumLabel) > 0 {
		labels = labels[:0]
		for k, v := range sample.NumLabel {
			labels = append(labels, fmt.Sprintf("%q%x%x", k, v, sample.NumUnit[k]))
		}
		sort.Strings(labels)
		for _, label := range labels {
			dgst.hash.WriteString(label)
		}
	}

	return dgst.hash.Sum64()
}

func collectLocations(sample *pprofProfile.Sample, locs []string, lines []uint16) ([]string, []string, []uint16) {
	nlocs := len(sample.Location)
	if cap(locs) < nlocs*2 {
		panic(fmt.Sprintf("locs slice is under capacity: want %d, got %d", cap(locs), nlocs*2))
	}
	// point funcs and files to locs, allowing to reuse the slice
	funcs := locs[:0:nlocs]
	files := locs[nlocs:nlocs]
	lines = lines[:0]
	for _, loc := range sample.Location {
		// FIXME(narqo) always uses first location line; i.e. loses information about inlined functions
		line := loc.Line[0]
		funcs = append(funcs, line.Function.Name)
		files = append(files, line.Function.Filename)
		lines = append(lines, uint16(line.Line))
	}
	return funcs, files, lines
}

// supports only profiles string labels
func collectLabels(sample *pprofProfile.Sample, keys []string, svals []string) ([]string, []string) {
	keys = keys[:0]
	svals = svals[:0]
	for k, vv := range sample.Label {
		for _, v := range vv {
			keys = append(keys, k)
			svals = append(svals, v)
		}
	}
	return keys, svals
}

/*
-- pprof top
select flat, cum, func
from (
    select sum(values[1])        as flat,
        `locations.func_name`[1] as func
    from pprof_samples
    group by func
) t1
join (
    select sum(values[1])     as cum,
        `locations.func_name` as func
    from pprof_samples
        array join `locations.func_name`
    group by func
) t2 using func
order by flat desc;

-- merge profiles
select
	sumForEach(values) as values,
	any(`locations.func_name`) as funcs
from pprof_samples
group by digest;

-- diff profiles
select
    groupArray(values[-1]) as cpu_vals,
    arrayEnumerate(cpu_vals) as indexes,
    arrayMap(i -> cpu_vals[i] - cpu_vals[i-1], indexes) as diffs,
    any(funcs) as funcs
FROM (
    SELECT digest, values, `locations.func_name` as funcs
    FROM pprof_samples
     ORDER BY profile_key)
GROUP BY digest;

*/
