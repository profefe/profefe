package postgres

import (
	"context"
	"database/sql"
	"fmt"
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

func (st *pqStorage) CreateService(ctx context.Context, svc *profile.Service) error {
	st.logger.Debugw("createService", "service", svc)

	_, err := st.db.ExecContext(
		ctx,
		sqlInsertService,
		svc.BuildID,
		svc.Token.String(),
		svc.Name,
		svc.CreatedAt,
		ServiceLabels(svc.Labels),
	)
	if err != nil {
		err = fmt.Errorf("could not insert %v into services: %v", svc, err)
	}
	return err
}

func (st *pqStorage) GetServices(ctx context.Context, filter *profile.GetServicesFilter) ([]*profile.Service, error) {
	defer func(t time.Time) {
		st.logger.Debugw("getServices", "time", time.Since(t))
	}(time.Now())

	var (
		rows *sql.Rows
		err  error
	)
	if filter.Service == "" {
		rows, err = st.db.QueryContext(ctx, sqlSelectServices)
	} else {
		rows, err = st.db.QueryContext(ctx, sqlSelectServicesByName, filter.Service)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query services (%v): %v", filter, err)
	}

	defer rows.Close()

	return st.getServices(ctx, rows)
}

func (st *pqStorage) getServices(ctx context.Context, rows *sql.Rows) ([]*profile.Service, error) {
	var svcs []*profile.Service
	for rows.Next() {
		var (
			name      string
			createdAt time.Time
			labels    ServiceLabels
		)
		err := rows.Scan(&name, &createdAt, &labels)
		if err != nil {
			return nil, err
		}

		svc := &profile.Service{
			Name:      name,
			CreatedAt: createdAt,
			Labels:    profile.Labels(labels),
		}
		svcs = append(svcs, svc)
	}

	if len(svcs) == 0 {
		return nil, profile.ErrNotFound
	}

	return svcs, nil
}

func (st *pqStorage) CreateProfile(ctx context.Context, prof *profile.Profile, pp *pprofProfile.Profile) error {
	queryBuilder, err := sqlSamplesQueryBuilder(prof.Type)
	if err != nil {
		return err
	}

	defer func(t time.Time) {
		st.logger.Debugw("createProfile", "profile", prof, "time", time.Since(t))
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

	var profID int64
	err = tx.QueryRowContext(
		ctx,
		sqlInsertProfiles,
		prof.Service.BuildID,
		prof.Service.Token.String(),
		time.Unix(0, pp.TimeNanos),
		int(prof.Type),
		pp.Period,
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
		return fmt.Errorf("could not commit transaction: %v", err)
	}

	return nil
}

func (st *pqStorage) insertProfLocations(ctx context.Context, tx *sql.Tx, locs []*pprofProfile.Location) (locIDs []int64, err error) {
	err = copyLocations(ctx, st.logger, tx, locs)
	if err != nil {
		return nil, fmt.Errorf("could not copy locations: %v", err)
	}

	_, err = tx.ExecContext(ctx, sqlInsertFunctions)
	if err != nil {
		return nil, fmt.Errorf("could not insert functions: %v", err)
	}

	_, err = tx.ExecContext(ctx, sqlInsertMappings)
	if err != nil {
		return nil, fmt.Errorf("could not insert mappings: %v", err)
	}

	locIDs = make([]int64, 0, len(locs))

	defer func(t time.Time) {
		st.logger.Debugw("insertProfLocations", logger.MultiLine("query", sqlInsertLocations), "nlocids", len(locIDs), "time", time.Since(t))
	}(time.Now())

	rows, err := tx.QueryContext(ctx, sqlInsertLocations)
	if err != nil {
		return nil, fmt.Errorf("could execute locations query: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var locID int64
		err := rows.Scan(&locID)
		if err != nil {
			return nil, fmt.Errorf("could scan locations query: %v", err)
		}
		locIDs = append(locIDs, locID)
	}

	return locIDs, rows.Err()
}

func (st *pqStorage) insertProfSamples(ctx context.Context, tx *sql.Tx, query string, profID int64, locIDs pq.Int64Array, samples []*pprofProfile.Sample) error {
	copyStmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("could not prepare statement: %v", err)
	}
	defer copyStmt.Close()

	defer func(t time.Time) {
		st.logger.Debugw("insertProfSamples", logger.MultiLine("query", query), "profid", profID, "nsamples", len(samples), "time", time.Since(t))
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
			return fmt.Errorf("could not exec sql statement: %v", err)
		}

		sampleLocIDs = sampleLocIDs[:0]
		args = args[:0]
	}
	_, err = copyStmt.ExecContext(ctx)
	if err != nil {
		err = fmt.Errorf("could not finalize statement: %v", err)
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

func (st *pqStorage) GetProfile(ctx context.Context, filter *profile.GetProfileFilter) (*pprofProfile.Profile, error) {
	defer func(t time.Time) {
		st.logger.Debugw("getProfile", "time", time.Since(t))
	}(time.Now())

	return st.getProfile(ctx, filter)
}

func (st *pqStorage) getProfile(ctx context.Context, filter *profile.GetProfileFilter) (*pprofProfile.Profile, error) {
	queryBuilder, err := sqlSamplesQueryBuilder(filter.Type)
	if err != nil {
		return nil, err
	}

	args := make([]interface{}, 0)
	whereParts := make([]string, 0)
	if filter.Service != "" {
		args = append(args, filter.Service)
		whereParts = append(whereParts, "v.name = $1") // v is for "services AS v" in select query
	}

	if !filter.CreatedAtMin.IsZero() && !filter.CreatedAtMax.IsZero() {
		args = append(args, filter.CreatedAtMin, filter.CreatedAtMax)
		whereParts = append(whereParts, "p.created_at BETWEEN $2 AND $3") // p is for "profiles AS p" in select query
	}

	for _, label := range filter.Labels {
		args = append(args, label.Value)
		whereParts = append(whereParts, fmt.Sprintf("v.labels ->> '%s' = $%d", label.Key, len(args)))
	}

	pb := pprofutil.NewProfileBuilder(filter.Type)
	// set of uniq pprof.Locations associated with samples
	locSet := make(map[int64]*pprofProfile.Location)

	query := queryBuilder.ToSelectSQL(whereParts...)
	err = st.selectProfileSamples(ctx, filter.Type, pb, locSet, query, args)
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

	return pb.Build()
}

func (st *pqStorage) selectProfileSamples(
	ctx context.Context,
	ptyp profile.ProfileType,
	pb *pprofutil.ProfileBuilder,
	locSet map[int64]*pprofProfile.Location,
	query string,
	args []interface{},
) error {
	st.logger.Debugw("selectProfileSamples", logger.MultiLine("query", query), "args", args)

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
				loc = &pprofProfile.Location{}
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
	st.logger.Debugw("selectProfileLocations", logger.MultiLine("query", query), "args", args)

	rows, err := st.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("faield to query locations (%v): %v", args, err)
	}
	defer rows.Close()

	funcSet := make(map[int64]*pprofProfile.Function)

	for rows.Next() {
		var (
			lr LocationRecord
			mr MappingRecord
			fr FunctionRecord
		)
		err := rows.Scan(&lr.LocationID, &mr, &lr.Address, &lr.Line, &fr.ID, &fr.FuncName, &fr.FileName)
		if err != nil {
			return err
		}

		loc := locSet[lr.LocationID]
		if loc == nil {
			return fmt.Errorf("found unexpected location record %v", lr)
		}

		if loc.Mapping == nil {
			m := &pprofProfile.Mapping{
				Start:   mr.MemStart,
				Limit:   mr.MemLimit,
				Offset:  mr.Offset,
				File:    mr.File,
				BuildID: mr.BuildID,
			}
			pb.AddMapping(m)

			loc.Mapping = m
			loc.Address = lr.Address
		}

		fn := funcSet[fr.ID]
		if fn == nil {
			// as for Go 1.12 Function.start_line never got populated by runtime/pprof
			// see https://github.com/golang/go/blob/5ee1b849592787ed050ef3fbd9b2c58aabd20ff3/src/runtime/pprof/proto.go
			fn = &pprofProfile.Function{
				Name:       fr.FuncName,
				SystemName: fr.FuncName,
				Filename:   fr.FileName,
			}
			pb.AddFunction(fn)
			funcSet[fr.ID] = fn
		}

		// "multiple line indicates this location has inlined functions" (see profile.proto)
		line := pprofProfile.Line{
			Function: fn,
			Line:     lr.Line,
		}
		loc.Line = append(loc.Line, line)
	}

	return rows.Err()
}

func (st *pqStorage) DeleteProfile(ctx context.Context, prof *profile.Profile) error {
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
