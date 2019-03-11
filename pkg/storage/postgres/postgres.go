package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"time"

	"github.com/lib/pq"
	"github.com/profefe/profefe/internal/pprof"
	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/logger"
	"github.com/profefe/profefe/pkg/profile"
)

type pqStorage struct {
	logger *logger.Logger
	db     *sql.DB
}

func New(log *logger.Logger, db *sql.DB) (profile.Storage, error) {
	s := &pqStorage{
		logger: log,
		db:     db,
	}
	return s, nil
}

func (st *pqStorage) Create(ctx context.Context, prof *profile.Profile) error {
	_, err := st.db.ExecContext(
		ctx,
		sqlInsertService,
		prof.Service.BuildID,
		prof.Service.Token.String(),
		prof.Service.Name,
		prof.CreatedAt,
		hstoreFromLabels(prof.Service.Labels),
	)
	if err != nil {
		err = fmt.Errorf("could not insert %v into services: %v", prof, err)
	}
	return err
}

func (st *pqStorage) Update(ctx context.Context, prof *profile.Profile, r io.Reader) error {
	pp, err := pprofProfile.Parse(r)
	if err != nil {
		return fmt.Errorf("could not parse profile: %v", err)
	}

	return st.updateProfile(ctx, prof, pp)
}

func (st *pqStorage) updateProfile(ctx context.Context, prof *profile.Profile, pp *pprofProfile.Profile) error {
	var sqlSamples sqlSamplesBuilder
	switch prof.Type {
	case profile.CPUProfile:
		sqlSamples = sqlSamplesCPU
	case profile.HeapProfile:
		sqlSamples = sqlSamplesHeap
	default:
		return fmt.Errorf("profile type %v is not supported", prof.Type)
	}

	defer func(t time.Time) {
		st.logger.Debugw("update profile", "time", time.Since(t))
	}(time.Now())

	tx, err := st.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, sqlCreateTempTable)
	if err != nil {
		return fmt.Errorf("could not create temp table %q: %v", sqlCreateTempTable, err)
	}

	copyStmt, err := tx.PrepareContext(ctx, sqlCopyTable)
	if err != nil {
		return fmt.Errorf("could not prepare COPY statement %q: %v", sqlCopyTable, err)
	}

	err = st.copyProfSamples(ctx, copyStmt, pp.Sample)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, sqlInsertLocations)
	if err != nil {
		return fmt.Errorf("could not insert locations: %v", err)
	}

	_, err = tx.ExecContext(
		ctx,
		sqlSamples.BuildInsertQuery(),
		prof.Service.BuildID,
		prof.Service.Token.String(),
		time.Unix(0, pp.TimeNanos),
	)
	if err != nil {
		return fmt.Errorf("could not insert samples: %v", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %v", err)
	}

	return nil
}

func (st *pqStorage) copyProfSamples(ctx context.Context, stmt *sql.Stmt, samples []*pprofProfile.Sample) error {
	defer func(t time.Time) {
		st.logger.Debugw("copy samples", "time", time.Since(t))
	}(time.Now())

	for sampleID, sample := range samples {
		labels := st.getSampleLabels(sample)

		for _, loc := range sample.Location {
			for _, ln := range loc.Line {
				_, err := stmt.ExecContext(
					ctx,
					sampleID,
					ln.Function.Name,
					ln.Function.Filename,
					ln.Line,
					pq.Int64Array(sample.Value),
					labels,
				)
				if err != nil {
					return fmt.Errorf("could not exec COPY statement: %v", err)
				}
			}
		}
	}
	_, err := stmt.ExecContext(ctx)
	if err != nil {
		err = fmt.Errorf("could not finalize COPY statement: %v", err)
	}
	return err
}

// returns at most one label for a key
func (st *pqStorage) getSampleLabels(sample *pprofProfile.Sample) (labels SampleLabels) {
	for k, v := range sample.Label {
		labels = append(labels, SampleLabel{Key: k, ValueStr: v[0]})
	}
	for k, v := range sample.NumLabel {
		labels = append(labels, SampleLabel{Key: k, ValueNum: v[0]})
	}
	return labels
}

func (st *pqStorage) Query(ctx context.Context, queryReq *profile.QueryRequest) (io.Reader, error) {
	defer func(t time.Time) {
		st.logger.Debugw("query samples", "time", time.Since(t))
	}(time.Now())

	return st.queryByCreatedAt(ctx, queryReq)
}

func (st *pqStorage) queryByCreatedAt(ctx context.Context, queryReq *profile.QueryRequest) (io.Reader, error) {
	args := []interface{}{
		queryReq.Service,
		queryReq.CreatedAtMin,
		queryReq.CreatedAtMax,
	}
	whereParts := make([]string, 0, len(queryReq.Labels))
	for _, label := range queryReq.Labels {
		args = append(args, label.Value)
		whereParts = append(whereParts, fmt.Sprintf("v.labels->'%s' = $%d", label.Key, len(args))) // v.labels is for "services AS v" in the select query
	}

	var (
		buf bytes.Buffer
		err error
	)

	switch queryReq.Type {
	case profile.CPUProfile:
		query := sqlSamplesCPU.BuildSelectQuery(whereParts...)
		err = st.queryCPUSamples(ctx, query, args...)
	}

	return &buf, err
}

func (st *pqStorage) queryCPUSamples(ctx context.Context, query string, args ...interface{}) (samples []SampleCPURecord, error error) {
	samplesRows, err := st.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("faield to query samples (%v): %v", args, err)
	}
	defer samplesRows.Close()

	locSet := make(map[int64]struct{})
	locs := make(pq.Int64Array, 0)

	for samplesRows.Next() {
		var s SampleCPURecord
		err = samplesRows.Scan(&s.ServiceID, &s.CreatedAt, &locs, &s.Labels, &s.SamplesCount, &s.CPUNanos)
		if err != nil {
			return nil, err
		}

		for _, loc := range locs {
			locSet[loc] = struct{}{}
		}

		s.Locations = locs
		locs = locs[:0]

		samples = append(samples, s)
	}

	if err := samplesRows.Err(); err != nil {
		return nil, err
	}

	if len(samples) == 0 {
		return nil, profile.ErrEmpty
	}

	locs = locs[:0]
	for loc := range locSet {
		locs = append(locs, loc)
	}
	locationsRows, err := st.db.QueryContext(ctx, sqlSelectLocations, locs)
	if err != nil {
		return nil, fmt.Errorf("faield to query locations (%v): %v", locs, err)
	}
	defer locationsRows.Close()

	for locationsRows.Next() {
		var l LocationRecord
		err := locationsRows.Scan(&l.LocationID, &l.FuncName, &l.FileName, &l.Line)
		if err != nil {
			return nil, err
		}
	}
}

func (st *pqStorage) XXXqueryByCreatedAt(ctx context.Context, queryReq *profile.QueryRequest) (io.Reader, error) {
	defer func(t time.Time) {
		st.logger.Debugw("query samples", "time", time.Since(t))
	}(time.Now())

	var (
		queryBuilder sqlSamplesBuilder
		scanner      profileRecordScanner
		writeProto   func(io.Writer, []pprof.ProfileRecord, pprof.LocMap) error
	)
	switch queryReq.Type {
	case profile.CPUProfile:
		queryBuilder = sqlSamplesCPU
		scanner.scanFunc = scanCPUProfileRecord
		writeProto = pprof.WriteCPUProto
	case profile.HeapProfile:
		queryBuilder = sqlSamplesHeap
		scanner.scanFunc = scanHeapProfileRecord
		writeProto = pprof.WriteHeapProto
	default:
		return nil, fmt.Errorf("profile type %v is not supported", queryReq.Type)
	}

	args := []interface{}{
		queryReq.Service,
		queryReq.CreatedAtMin,
		queryReq.CreatedAtMax,
	}

	var whereLabels string
	for _, label := range queryReq.Labels {
		args = append(args, label.Value)
		whereLabels += fmt.Sprintf(" AND v.labels->'%s' = $%d", label.Key, len(args))
	}

	query := queryBuilder.BuildSelectQuery(whereLabels)
	samplesRows, err := st.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer samplesRows.Close()

	locSet := make(map[int64]struct{})

	var (
		locs pq.Int64Array
		p    []pprof.ProfileRecord
	)
	for samplesRows.Next() {
		r := pprof.ProfileRecord{}
		locs := locs[:0]
		err := scanner.Scan(samplesRows, &r, &locs)
		if err != nil {
			return nil, err
		}
		for _, loc := range locs {
			r.Stack0 = append(r.Stack0, uint64(loc))
			locSet[loc] = struct{}{}
		}
		p = append(p, r)
	}
	if err := samplesRows.Err(); err != nil {
		return nil, err
	}

	if len(p) == 0 {
		return nil, profile.ErrEmpty
	}

	locs = locs[:0]
	for loc := range locSet {
		locs = append(locs, loc)
	}

	locRows, err := st.db.QueryContext(ctx, sqlSelectLocations, locs)
	if err != nil {
		return nil, err
	}
	defer locRows.Close()

	locMap := make(pprof.LocMap, 8)
	for locRows.Next() {
		var (
			locID uint64
			loc   pprof.Location
		)
		err := locRows.Scan(&locID, &loc.Function, &loc.File, &loc.Line)
		if err != nil {
			return nil, err
		}
		locMap[locID] = loc
	}
	if err := locRows.Err(); err != nil {
		return nil, err
	}

	// TODO(narqo): implement io.WriterTo
	var buf bytes.Buffer
	err = writeProto(&buf, p, locMap)
	return &buf, err
}

type scanProfileRecordFunc func(*sql.Rows, *pprof.ProfileRecord, *pq.Int64Array, *SampleLabels) error

type profileRecordScanner struct {
	labels   SampleLabels
	scanFunc scanProfileRecordFunc
}

func (s profileRecordScanner) Scan(rows *sql.Rows, r *pprof.ProfileRecord, locs *pq.Int64Array) error {
	labels := s.labels[:0]
	if err := s.scanFunc(rows, r, locs, &labels); err != nil {
		return err
	}
	for _, label := range labels {
		if label.Key == "" {
			continue
		}
		r.Labels = append(r.Labels, pprof.Label(label))
	}
	return nil
}

func scanCPUProfileRecord(rows *sql.Rows, r *pprof.ProfileRecord, locs *pq.Int64Array, labels *SampleLabels) error {
	var samples, cpu int64
	err := rows.Scan(&samples, &cpu, locs, labels)
	if err != nil {
		return err
	}
	r.Values = []int64{samples, cpu}
	return nil
}

func scanHeapProfileRecord(rows *sql.Rows, r *pprof.ProfileRecord, locs *pq.Int64Array, labels *SampleLabels) error {
	var (
		allocObjects, allocBytes int64
		inuseObjects, inuseBytes int64
	)
	err := rows.Scan(&allocObjects, &allocBytes, &inuseObjects, &inuseBytes, locs, labels)
	if err != nil {
		return err
	}
	r.Values = []int64{allocObjects, allocBytes, inuseObjects, inuseBytes}
	return nil
}

func (st *pqStorage) Delete(ctx context.Context, prof *profile.Profile) error {
	panic("implement me")
}
