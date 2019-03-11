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
	var sqlSamples samplesQueryBuilder
	switch prof.Type {
	case profile.CPUProfile:
		sqlSamples = sqlSamplesCPU
	case profile.HeapProfile:
		sqlSamples = sqlSamplesHeap
	default:
		return fmt.Errorf("profile type %v is not supported", prof.Type)
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
		sqlSamples.ToInsertSQL(),
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

	return st.getProfileByCreatedAt(ctx, queryReq)
}

func (st *pqStorage) getProfileByCreatedAt(ctx context.Context, queryReq *profile.QueryRequest) (io.Reader, error) {
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

	var sqlQuery string

	switch queryReq.Type {
	case profile.CPUProfile:
		sqlQuery = sqlSamplesCPU.ToSelectSQL(whereParts...)
	}

	pp, err := getPProfProfile(ctx, st.logger, st.db, queryReq.Type, sqlQuery, args)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := pprof.WriteCPUProto(&buf, pp); err != nil {
		return nil, err
	}

	return &buf, nil
}

func (st *pqStorage) Delete(ctx context.Context, prof *profile.Profile) error {
	panic("implement me")
}

func getPProfProfile(ctx context.Context, logger *logger.Logger, db *sql.DB, ptyp profile.ProfileType, query string, args []interface{}) (*pprof.Profile, error) {
	samplesRows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("faield to query samples (%v): %v", args, err)
	}
	defer samplesRows.Close()

	locSet := make(map[int64]struct{})

	var recs []pprof.ProfileRecord
	for samplesRows.Next() {
		var s BaseSampleRecord
		p, err := scanSampleRecord(samplesRows, ptyp, &s)
		if err != nil {
			return nil, err
		}

		recs = append(recs, p.ToPProf())

		for _, loc := range s.Locations {
			locSet[loc] = struct{}{}
		}
	}

	if err := samplesRows.Err(); err != nil {
		return nil, err
	}

	if len(recs) == 0 {
		return nil, profile.ErrEmpty
	}

	locs := make(pq.Int64Array, 0, len(locSet))
	for loc := range locSet {
		locs = append(locs, loc)
	}
	locationsRows, err := db.QueryContext(ctx, sqlSelectLocations, locs)
	if err != nil {
		return nil, fmt.Errorf("faield to query locations (%v): %v", locs, err)
	}
	defer locationsRows.Close()

	locMap := make(pprof.LocMap, 8)
	for locationsRows.Next() {
		var l LocationRecord
		err := locationsRows.Scan(&l.LocationID, &l.FuncName, &l.FileName, &l.Line)
		if err != nil {
			return nil, err
		}

		locMap[l.LocationID] = l.ToPProf()
	}

	p := &pprof.Profile{
		Records:   recs,
		Locations: locMap,
	}
	return p, nil
}

type toPProfProfileRecorder interface {
	ToPProf() pprof.ProfileRecord
}

func scanSampleRecord(rows *sql.Rows, typ profile.ProfileType, s *BaseSampleRecord) (p toPProfProfileRecorder, err error) {
	dest := []interface{}{
		&s.ServiceID,
		&s.CreatedAt,
		&s.Locations,
		&s.Labels,
	}

	switch typ {
	case profile.CPUProfile:
		sr := &SampleCPURecord{
			BaseSampleRecord: s,
		}
		dest = append(dest, &sr.SamplesCount, &sr.CPUNanos)
		p = sr
	case profile.HeapProfile:
		sr := &SampleHeapRecord{
			BaseSampleRecord: s,
		}
		dest = append(dest, &sr.AllocObjects, &sr.AllocBytes, &sr.InuseObjects, &sr.InuseBytes)
		p = sr
	}

	err = rows.Scan(dest...)
	return p, err
}
