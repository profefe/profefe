// +build postgres

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lib/pq"
	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/pprofutil"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
	"golang.org/x/xerrors"
)

// DEPRECATED
// The implementation of storage on top of PostgreSQL is too inefficient and complex.
// It's left as is (likely incompatible with pkg/storage.Storage interface) only for references.
// See the description of https://github.com/profefe/profefe/pull/28 for "deprecation" background.
type Storage struct {
	logger *log.Logger
	db     *sql.DB
}

func New(logger *log.Logger, db *sql.DB) (*Storage, error) {
	st := &Storage{
		logger: logger,
		db:     db,
	}
	return st, nil
}

func (st *Storage) WriteProfile(ctx context.Context, meta *profile.Meta, pf *profile.SingleProfileReader) error {
	pp, err := pf.Profile()
	if err != nil {
		return err
	}

	queryBuilder, err := sqlSamplesQueryBuilder(meta.Type)
	if err != nil {
		return err
	}

	defer func(t time.Time) {
		st.logger.Debugw("createProfile", "time", time.Since(t))
	}(time.Now())

	tx, err := st.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, sqlCreateLocationsTempTable)
	if err != nil {
		return err
	}

	var labelsID int64
	err = tx.QueryRowContext(
		ctx,
		sqlSOIProfileLabels,
		meta.Service,
		meta.InstanceID,
		ProfileLabels(meta.Labels),
	).Scan(&labelsID)
	if err != nil {
		return err
	}

	var profID int64
	err = tx.QueryRowContext(
		ctx,
		sqlInsertProfiles,
		time.Unix(0, pp.TimeNanos),
		meta.CreatedAt,
		int(meta.Type),
		pp.Period,
		labelsID,
	).Scan(&profID)
	if err != nil {
		return err
	}

	locIDs, err := st.insertProfLocations(ctx, tx, pp.Location)
	if err != nil {
		return err
	}

	sqlInsertSamples := queryBuilder.ToInsertSQL()
	err = st.insertProfSamples(ctx, tx, sqlInsertSamples, profID, locIDs, pp.Sample)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return xerrors.Errorf("could not commit transaction: %w", err)
	}

	return nil
}

func (st *Storage) insertProfLocations(ctx context.Context, tx *sql.Tx, locs []*pprofProfile.Location) (locIDs []int64, err error) {
	err = copyLocations(ctx, st.logger, tx, locs)
	if err != nil {
		return nil, xerrors.Errorf("could not copy locations: %w", err)
	}

	_, err = tx.ExecContext(ctx, sqlInsertFunctions)
	if err != nil {
		return nil, xerrors.Errorf("could not insert functions: %w", err)
	}

	_, err = tx.ExecContext(ctx, sqlInsertMappings)
	if err != nil {
		return nil, xerrors.Errorf("could not insert mappings: %w", err)
	}

	locIDs = make([]int64, 0, len(locs))

	defer func(t time.Time) {
		st.logger.Debugw("insertProfLocations", log.MultiLine("query", sqlInsertLocations), "nlocids", len(locIDs), "time", time.Since(t))
	}(time.Now())

	rows, err := tx.QueryContext(ctx, sqlInsertLocations)
	if err != nil {
		return nil, xerrors.Errorf("could not execute locations query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var locID int64
		err := rows.Scan(&locID)
		if err != nil {
			return nil, xerrors.Errorf("could not scan locations query: %w", err)
		}
		locIDs = append(locIDs, locID)
	}

	return locIDs, rows.Err()
}

func (st *Storage) insertProfSamples(ctx context.Context, tx *sql.Tx, query string, profID int64, locIDs pq.Int64Array, samples []*pprofProfile.Sample) error {
	copyStmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return xerrors.Errorf("could not prepare statement: %w", err)
	}
	defer copyStmt.Close()

	defer func(t time.Time) {
		st.logger.Debugw("insertProfSamples", log.MultiLine("query", query), "profid", profID, "nsamples", len(samples), "time", time.Since(t))
	}(time.Now())

	var (
		args         []interface{}
		sampleLocIDs pq.Int64Array
	)

	for _, sample := range samples {
		labels := getSampleLabels(sample)

		for _, loc := range sample.Location {
			n := loc.ID
			locID := locIDs[n-1]
			sampleLocIDs = append(sampleLocIDs, locID)
		}

		args = append(args, profID, sampleLocIDs, labels)
		for _, value := range sample.Value {
			var v sql.NullInt64
			if value > 0 {
				v = sql.NullInt64{value, true}
			}
			args = append(args, v)
		}

		_, err = copyStmt.ExecContext(ctx, args...)
		if err != nil {
			return xerrors.Errorf("could not exec sql statement: %w", err)
		}

		sampleLocIDs = sampleLocIDs[:0]
		args = args[:0]
	}
	_, err = copyStmt.ExecContext(ctx)
	if err != nil {
		err = xerrors.Errorf("could not finalize statement: %w", err)
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

func (st *Storage) GetProfile(ctx context.Context, pid profile.ID) (*profile.SingleProfileReader, error) {
	panic("implement me")
}

func (st *Storage) FindProfile(ctx context.Context, req *storage.FindProfilesParams) (*profile.SingleProfileReader, error) {
	defer func(t time.Time) {
		st.logger.Debugw("findProfile", "time", time.Since(t))
	}(time.Now())

	pp, err := st.findProfile(ctx, req)
	if err != nil {
		return nil, err
	}
	return profile.NewSingleProfileReader(pp), nil
}

func (st *Storage) findProfile(ctx context.Context, req *storage.FindProfilesParams) (*pprofProfile.Profile, error) {
	queryBuilder, err := sqlSamplesQueryBuilder(req.Type)
	if err != nil {
		return nil, err
	}

	args := make([]interface{}, 0)
	whereParts := make([]string, 0)
	if req.Service != "" {
		args = append(args, req.Service)
		whereParts = append(whereParts, "v.service = $1") // v is for "pprof_profile_labels AS v" in select query
	}

	if !req.CreatedAtMin.IsZero() && !req.CreatedAtMax.IsZero() {
		args = append(args, req.CreatedAtMin, req.CreatedAtMax)
		whereParts = append(whereParts, "p.created_at >= $2 AND p.created_at < $3") // p is for "profiles AS p" in select query
	}

	for _, label := range req.Labels {
		args = append(args, label.Value)
		whereParts = append(whereParts, fmt.Sprintf("v.labels ->> '%s' = $%d", label.Key, len(args)))
	}

	sqlSelectSamples := queryBuilder.ToSelectSQL(whereParts...)
	st.logger.Debugw("selectProfileSamples", log.MultiLine("query", sqlSelectSamples), "args", args)

	rows, err := st.db.QueryContext(ctx, sqlSelectSamples, args...)
	if err != nil {
		return nil, xerrors.Errorf("could not query samples (%v): %w", args, err)
	}
	defer rows.Close()

	locSet := make(map[int64]*pprofProfile.Location)
	mapSet := make(map[int64]*pprofProfile.Mapping)
	funcSet := make(map[int64]*pprofProfile.Function)
	pb := pprofutil.NewProfileBuilder(req.Type)

	rs := newSampleRecordsScanner(req.Type)
	for rows.Next() {
		err := rs.ScanFrom(rows)
		if err != nil {
			return nil, err
		}

		sample := &pprofProfile.Sample{
			Value: rs.Value(),
		}
		for _, label := range rs.sampleRec.Labels {
			pprofutil.SampleAddLabel(sample, label.Key, label.ValueStr, label.ValueNum)
		}
		for _, locID := range rs.sampleRec.Locations {
			loc := locSet[locID]
			if loc == nil {
				loc = &pprofProfile.Location{}
				locSet[locID] = loc

				pb.AddLocation(loc)
			}
			sample.Location = append(sample.Location, loc)
		}

		pb.AddSample(sample)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if pb.IsEmpty() {
		return nil, storage.ErrEmpty
	}

	locIDs := make(pq.Int64Array, 0, len(locSet))
	for locID := range locSet {
		locIDs = append(locIDs, locID)
	}

	args = append(args[:0], locIDs)
	st.logger.Debugw("selectProfileLocations", log.MultiLine("query", sqlSelectLocations), "args", args)

	rows, err = st.db.QueryContext(ctx, sqlSelectLocations, args...)
	if err != nil {
		return nil, xerrors.Errorf("could not query locations (%v): %w", args, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			lr LocationRecord
			mr MappingRecord
			fr FunctionRecord
		)
		err := rows.Scan(&lr.LocationID, &mr.MappingID, &mr.Mapping, &lr.Address, &lr.Line, &fr.FuncID, &fr.FuncName, &fr.FileName)
		if err != nil {
			return nil, err
		}

		loc := locSet[lr.LocationID]
		if loc == nil {
			return nil, xerrors.Errorf("found unexpected location record %v: location %d not found in locations set", lr, lr.LocationID)
		}

		m := mapSet[mr.MappingID]
		if m == nil {
			m = &pprofProfile.Mapping{
				Start:           mr.Mapping.MemStart,
				Limit:           mr.Mapping.MemLimit,
				Offset:          mr.Mapping.Offset,
				File:            mr.Mapping.File,
				BuildID:         mr.Mapping.BuildID,
				HasFunctions:    mr.Mapping.HasFunctions,
				HasFilenames:    mr.Mapping.HasFilenames,
				HasLineNumbers:  mr.Mapping.HasLineNumbers,
				HasInlineFrames: mr.Mapping.HasInlineFrames,
			}
			mapSet[mr.MappingID] = m

			pb.AddMapping(m)
		}

		loc.Mapping = m
		loc.Address = lr.Address

		fn := funcSet[fr.FuncID]
		if fn == nil {
			// as for Go 1.12 Function.start_line never got populated by runtime/pprof
			// see https://github.com/golang/go/blob/5ee1b849592787ed050ef3fbd9b2c58aabd20ff3/src/runtime/pprof/proto.go
			fn = &pprofProfile.Function{
				Name:       fr.FuncName,
				SystemName: fr.FuncName,
				Filename:   fr.FileName,
			}
			funcSet[fr.FuncID] = fn

			pb.AddFunction(fn)
		}

		// "multiple line indicates this location has inlined functions" (see pprof/profile.proto)
		line := pprofProfile.Line{
			Function: fn,
			Line:     lr.Line,
		}
		loc.Line = append(loc.Line, line)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return pb.Build()
}

//func (st *Storage) DeleteProfile(ctx context.Context, prof *profile.Profile) error {
//	panic("implement me")
//}

func sqlSamplesQueryBuilder(ptyp profile.ProfileType) (qb samplesQueryBuilder, err error) {
	switch ptyp {
	case profile.CPUProfile:
		return sqlSamplesCPU, nil
	case profile.HeapProfile:
		return sqlSamplesHeap, nil
	}

	return qb, xerrors.Errorf("profile type %v is not supported", ptyp)
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
		&rec.ProfileID,
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
