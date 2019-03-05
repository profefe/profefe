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
	_ = sqlSamples

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

	insertSamplesStmt, err := tx.PrepareContext(ctx, sqlSamples.BuildInsertQuery())
	if err != nil {
		return fmt.Errorf("could not prepare INSERT statement: %v", err)
	}

	_, err = insertSamplesStmt.ExecContext(
		ctx,
		prof.Service.BuildID,
		prof.Service.Token.String(),
		time.Unix(0, pp.TimeNanos),
	)
	if err != nil {
		return fmt.Errorf("could not insert samples: %v", err)
	}

	if err := insertSamplesStmt.Close(); err != nil {
		return fmt.Errorf("could not close INSERT statement: %v", err)
	}

	if err := copyStmt.Close(); err != nil {
		return fmt.Errorf("could not close COPY statement: %v", err)
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
func (st *pqStorage) getSampleLabels(sample *pprofProfile.Sample) (labels sampleLabels) {
	for k, v := range sample.Label {
		labels = append(labels, sampleLabel{Key: k, ValueStr: v[0]})
	}
	for k, v := range sample.NumLabel {
		labels = append(labels, sampleLabel{Key: k, ValueNum: v[0]})
	}
	return labels
}

func (st *pqStorage) Query(ctx context.Context, queryReq *profile.QueryRequest) (io.Reader, error) {
	return st.queryByCreatedAt(ctx, queryReq)
}

func (st *pqStorage) queryByCreatedAt(ctx context.Context, queryReq *profile.QueryRequest) (io.Reader, error) {
	var sqlSamples sqlSamplesBuilder
	switch queryReq.Type {
	case profile.CPUProfile:
		sqlSamples = sqlSamplesCPU
	case profile.HeapProfile:
		sqlSamples = sqlSamplesHeap
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

	query := sqlSamples.BuildSelectQuery(whereLabels)
	selectSamplesStmt, err := st.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("could not prepare SELECT samples statement: %v", err)
	}

	samplesRows, err := selectSamplesStmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	defer samplesRows.Close()

	locSet := make(map[int64]struct{})
	var locs pq.Int64Array

	var p []pprof.MemProfileRecord
	for samplesRows.Next() {
		r := pprof.MemProfileRecord{}
		var labels sampleLabels
		locs = locs[:0]
		err := samplesRows.Scan(&r.AllocObjects, &r.AllocBytes, &r.InUseObjects, &r.InUseBytes, &locs, &labels)
		if err != nil {
			return nil, err
		}
		for _, loc := range locs {
			r.Stack0 = append(r.Stack0, uint64(loc))
			locSet[loc] = struct{}{}
		}
		for _, label := range labels {
			if label.Key == "" {
				continue
			}
			r.Labels = append(r.Labels, pprof.Label(label))
		}
		p = append(p, r)
	}
	if err := samplesRows.Err(); err != nil {
		return nil, err
	}

	if err := selectSamplesStmt.Close(); err != nil {
		return nil, fmt.Errorf("could not close SELECT samples statement: %v", err)
	}

	selectLocsStmt, err := st.db.PrepareContext(ctx, sqlSelectLocations)
	if err != nil {
		return nil, fmt.Errorf("could not prepare SELECT locations statement: %v", err)
	}
	for loc := range locSet {
		locs = append(locs, loc)
	}

	locRows, err := selectLocsStmt.QueryContext(ctx, locs)
	if err != nil {
		return nil, err
	}
	defer locRows.Close()

	locMap := make(map[uint64]pprof.Location, 8)
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
	err = pprof.WriteHeapProto(&buf, p, locMap)
	return &buf, err
}

func (st *pqStorage) Delete(ctx context.Context, prof *profile.Profile) error {
	panic("implement me")
}
