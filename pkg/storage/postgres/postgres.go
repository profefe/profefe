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
	pprof, err := pprofProfile.Parse(r)
	if err != nil {
		return fmt.Errorf("could not parse profile: %v", err)
	}

	return st.updateProfile(ctx, prof, pprof)
}

func (st *pqStorage) updateProfile(ctx context.Context, prof *profile.Profile, pprof *pprofProfile.Profile) error {
	var sqlInsertSamples string
	switch prof.Type {
	case profile.CPUProfile:
		sqlInsertSamples = sqlSamplesCPU.InsertQuery()
	case profile.HeapProfile:
		sqlInsertSamples = sqlSamplesHeap.InsertQuery()
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

	err = st.copyProfSamples(ctx, copyStmt, pprof.Sample)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, sqlInsertLocations)
	if err != nil {
		return fmt.Errorf("could not insert locations: %v", err)
	}

	insertSamplesStmt, err := tx.PrepareContext(ctx, sqlInsertSamples)
	if err != nil {
		return fmt.Errorf("could not prepare INSERT statement %q: %v", sqlInsertSamples, err)
	}

	_, err = insertSamplesStmt.ExecContext(
		ctx,
		prof.Service.BuildID,
		prof.Service.Token.String(),
		time.Unix(0, pprof.TimeNanos),
		prof.ReceivedAt,
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
		for _, loc := range sample.Location {
			for _, ln := range loc.Line {
				_, err := stmt.ExecContext(
					ctx,
					sampleID,
					ln.Function.Name,
					ln.Function.Filename,
					ln.Line,
					pq.Array(sample.Value),
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

func (st *pqStorage) Query(ctx context.Context, queryReq *profile.QueryRequest) (io.Reader, error) {
	return st.queryByCreatedAt(ctx, queryReq)
}

func (st *pqStorage) queryByCreatedAt(ctx context.Context, queryReq *profile.QueryRequest) (io.Reader, error) {
	var sqlSelectSamples string
	switch queryReq.Type {
	case profile.CPUProfile:
		sqlSelectSamples = sqlSamplesCPU.SelectQuery()
	case profile.HeapProfile:
		sqlSelectSamples = sqlSamplesHeap.SelectQuery()
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

	query := fmt.Sprintf(sqlSelectSamples, whereLabels)
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

	var p []pprof.Runtime_MemProfileRecord
	for samplesRows.Next() {
		r := pprof.Runtime_MemProfileRecord{}
		locs = locs[:0]
		err := samplesRows.Scan(&r.AllocObjects, &r.AllocBytes, &r.InUseObjects, &r.InUseBytes, &locs)
		if err != nil {
			return nil, err
		}
		if len(locs) > len(r.Stack0) {
			// TODO(narqo): figure out how this happens
			//st.logger.Debugf("locations: %v", locs)
			locs = locs[:len(r.Stack0)]
		}
		for i, addr := range locs {
			r.Stack0[i] = uint64(addr)
			locSet[addr] = struct{}{}
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
	for addr := range locSet {
		locs = append(locs, addr)
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
