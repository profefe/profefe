package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"time"

	"github.com/lib/pq"
	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/logger"
	"github.com/profefe/profefe/pkg/pprofutil"
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
	st.logger.Debugw("create profile", "profile", prof)

	_, err := st.db.ExecContext(
		ctx,
		sqlInsertService,
		prof.Service.BuildID,
		prof.Service.Token.String(),
		prof.Service.Name,
		prof.Service.CreatedAt,
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
	queryBuilder, err := sqlSamplesQueryBuilder(prof.Type)
	if err != nil {
		return err
	}

	defer func(t time.Time) {
		st.logger.Debugw("update profile", "profile", prof, "time", time.Since(t))
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

	st.logger.Debugw("copy samples", logger.MultiLine("query", sqlCopyTable))
	copyStmt, err := tx.PrepareContext(ctx, sqlCopyTable)
	if err != nil {
		return fmt.Errorf("could not prepare COPY statement: %v", err)
	}

	err = st.copyProfSamples(ctx, copyStmt, pp.Sample)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, sqlInsertLocations)
	if err != nil {
		return fmt.Errorf("could not insert locations: %v", err)
	}

	sqlInsertSamples := queryBuilder.ToInsertSQL()
	st.logger.Debugw("insert samples", logger.MultiLine("query", sqlInsertSamples))

	_, err = tx.ExecContext(
		ctx,
		sqlInsertSamples,
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
		st.logger.Debugw("copy samples", "nsamples", len(samples), "time", time.Since(t))
	}(time.Now())

	for sampleID, sample := range samples {
		labels := getSampleLabels(sample)

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
func getSampleLabels(sample *pprofProfile.Sample) (labels SampleLabels) {
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
		st.logger.Debugw("query profile", "time", time.Since(t))
	}(time.Now())

	pp, err := st.getProfile(ctx, queryReq)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	err = pp.Write(&buf)
	return &buf, err
}

func (st *pqStorage) getProfile(ctx context.Context, queryReq *profile.QueryRequest) (*pprofProfile.Profile, error) {
	queryBuilder, err := sqlSamplesQueryBuilder(queryReq.Type)
	if err != nil {
		return nil, err
	}

	args := make([]interface{}, 0)
	whereParts := make([]string, 0)
	if queryReq.Service != "" {
		args = append(args, queryReq.Service)
		whereParts = append(whereParts, "v.name = $1") // v is for "services AS v" in select query
	}

	if !queryReq.CreatedAtMin.IsZero() && !queryReq.CreatedAtMax.IsZero() {
		args = append(args, queryReq.CreatedAtMin, queryReq.CreatedAtMax)
		whereParts = append(whereParts, "s.created_at BETWEEN $2 AND $3") // s is for "samples AS s" in select query
	}

	for _, label := range queryReq.Labels {
		args = append(args, label.Value)
		whereParts = append(whereParts, fmt.Sprintf("v.labels->'%s' = $%d", label.Key, len(args)))
	}

	pb := pprofutil.NewProfileBuilder(queryReq.Type)
	// set of uniq pprof.Locations associated with samples
	locSet := make(map[int64]*pprofProfile.Location)

	query := queryBuilder.ToSelectSQL(whereParts...)

	err = st.selectProfileSamples(ctx, queryReq.Type, pb, locSet, query, args)
	if err != nil {
		return nil, err
	}

	if pb.IsEmpty() {
		return nil, profile.ErrEmpty
	}

	locIDs := make(pq.Int64Array, 0, len(locSet))
	for locID := range locSet {
		locIDs = append(locIDs, locID)
	}

	args = args[:0]
	args = append(args, locIDs)
	err = st.selectProfileLocations(ctx, pb, locSet, sqlSelectLocations, args)
	if err != nil {
		return nil, err
	}

	return pb.Build(), nil
}

func (st *pqStorage) selectProfileSamples(
	ctx context.Context,
	ptyp profile.ProfileType,
	pb *pprofutil.ProfileBuilder,
	locSet map[int64]*pprofProfile.Location,
	query string,
	args []interface{},
) error {
	st.logger.Debugw("get profile samples", logger.MultiLine("query", query), "args", args)

	rows, err := st.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("faield to query samples (%v): %v", args, err)
	}
	defer rows.Close()

	rs := newSampleRecordsScanner(ptyp)
	for rows.Next() {
		err := rs.ScanFrom(rows)
		if err != nil {
			return err
		}

		s := &pprofProfile.Sample{
			Value: rs.Value(),
		}

		for _, label := range rs.sampleRec.Labels {
			pprofutil.SampleAddLabel(s, label.Key, label.ValueStr, label.ValueNum)
		}

		for _, lid := range rs.sampleRec.Locations {
			loc := locSet[lid]
			if loc == nil {
				loc = &pprofProfile.Location{
					Mapping: nil,
					Address: 0,
				}
				pb.AddLocation(loc)
				locSet[lid] = loc
			}
			s.Location = append(s.Location, loc)
		}

		pb.AddSample(s)
	}

	return rows.Err()
}

func (st *pqStorage) selectProfileLocations(
	ctx context.Context,
	pb *pprofutil.ProfileBuilder,
	locSet map[int64]*pprofProfile.Location,
	query string,
	args []interface{},
) error {
	st.logger.Debugw("get profile locations", logger.MultiLine("query", query), "args", args)

	rows, err := st.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("faield to query locations (%v): %v", args, err)
	}
	defer rows.Close()

	for rows.Next() {
		var l LocationRecord
		err := rows.Scan(&l.LocationID, &l.FuncName, &l.FileName, &l.Line)
		if err != nil {
			return err
		}

		loc := locSet[l.LocationID]
		if loc == nil {
			return fmt.Errorf("found unexpected location record %v", l)
		}

		fn := &pprofProfile.Function{
			Name:       l.FuncName,
			SystemName: l.FuncName,
			Filename:   l.FileName,
			StartLine:  l.Line,
		}
		pb.AddFunction(fn)

		line := pprofProfile.Line{
			Function: fn,
			Line:     l.Line,
		}
		loc.Line = append(loc.Line, line)
	}

	return rows.Err()
}

func (st *pqStorage) Delete(ctx context.Context, prof *profile.Profile) error {
	panic("implement me")
}

func sqlSamplesQueryBuilder(ptyp profile.ProfileType) (qb samplesQueryBuilder, err error) {
	switch ptyp {
	case profile.CPUProfile:
		return sqlSamplesCPU, nil
	case profile.HeapProfile:
		return sqlSamplesHeap, nil
	}

	return qb, fmt.Errorf("profile type %v is not supported", ptyp)
}

type sampleRecordsScanner struct {
	sampleRecordValuer

	sampleRec *BaseSampleRecord
	dest      []interface{}
}

func newSampleRecordsScanner(ptyp profile.ProfileType) *sampleRecordsScanner {
	var (
		rec    BaseSampleRecord
		valuer sampleRecordValuer
	)

	dest := []interface{}{
		&rec.ServiceID,
		&rec.CreatedAt,
		&rec.Locations,
		&rec.Labels,
	}

	switch ptyp {
	case profile.CPUProfile:
		sr := &SampleCPURecord{
			BaseSampleRecord: &rec,
		}
		dest = append(dest, &sr.SamplesCount, &sr.CPUNanos)
		valuer = sr
	case profile.HeapProfile:
		sr := &SampleHeapRecord{
			BaseSampleRecord: &rec,
		}
		dest = append(dest, &sr.AllocObjects, &sr.AllocBytes, &sr.InuseObjects, &sr.InuseBytes)
		valuer = sr
	}

	return &sampleRecordsScanner{
		valuer,
		&rec,
		dest,
	}
}

func (rs *sampleRecordsScanner) ScanFrom(rows *sql.Rows) error {
	return rows.Scan(rs.dest...)
}
