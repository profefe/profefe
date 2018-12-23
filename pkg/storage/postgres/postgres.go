package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	pprofProfile "github.com/google/pprof/profile"
	"github.com/lib/pq"
	"github.com/profefe/profefe/pkg/logger"
	"github.com/profefe/profefe/pkg/profile"
)

const (
	sqlInsertService = `
		INSERT INTO services (build_id, token, name, created_at, labels)
		VALUES ($1, $2, $3, $4, $5)`

	sqlCreateTempTable = `
		CREATE TEMPORARY TABLE IF NOT EXISTS profile_pprof_samples_tmp (sample_id INT, func_name TEXT, file_name TEXT, line INT, values_all BIGINT[]) 
		ON COMMIT DELETE ROWS;`

	sqlInsertLocations = `
		INSERT INTO profile_pprof_locations (func_name, file_name, line) 
		SELECT tmp.func_name, tmp.file_name, tmp.line 
		FROM profile_pprof_samples_tmp AS tmp 
		LEFT JOIN profile_pprof_locations l 
		ON tmp.func_name = l.func_name AND tmp.file_name = l.file_name AND tmp.line = l.line
		WHERE l.func_name IS NULL
		ON CONFLICT (func_name, file_name, line) DO NOTHING;`

	sqlInsertSamples = `
		INSERT INTO %[1]s (build_id, token, created_at, received_at, locations, %[2]s)
		SELECT s.build_id, s.token, s.created_at, s.received_at, locations, %[2]s
		FROM (values ($1, $2, $3::timestamp, $4::timestamp)) AS s (build_id, token, created_at, received_at),
	  	(
			SELECT sample_id, array_agg(l.location_id) as locations, %[3]s
			FROM profile_pprof_samples_tmp tmp
			INNER JOIN profile_pprof_locations l 
			ON tmp.func_name = l.func_name AND tmp.file_name = l.file_name AND tmp.line = l.line
			GROUP BY sample_id, %[2]s
		) AS t;`

	sqlSelectProfileByCreatedAt = `
		SELECT
			p.digest,
			p.type,
			p.size,
			p.created_at,
			p.received_at,
			p.build_id,
			p.token,
			s.labels
		FROM profiles_pprof p, services s
		WHERE p.build_id = s.build_id AND p.token = s.token 
		AND s.name = $1 AND p.type = $2
		%s
		AND p.created_at BETWEEN $3 AND $4
		ORDER BY p.created_at;`
)

var (
	sqlCopyTable = pq.CopyIn(
		"profile_pprof_samples_tmp",
		"sample_id",
		"func_name",
		"file_name",
		"line",
		"values_all",
	)
	sqlInsertSamplesCPU = createInsertSamples(
		"profile_pprof_samples_cpu",
		"value_cpu",
		"value_nanos",
	)
	sqlInsertSamplesHeap = createInsertSamples(
		"profile_pprof_samples_heap",
		"alloc_objects",
		"alloc_bytes",
		"inuse_objects",
		"inuse_bytes",
	)
	// TODO: more insert samples sql
	sqlInsertSamplesBlock = createInsertSamples(
		"profile_pprof_samples_block",
	)
	sqlInsertSamplesMutex = createInsertSamples(
		"profile_pprof_samples_mutex",
	)
)

const defaultProfilesLimit = 100

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
		sqlInsertSamples = sqlInsertSamplesCPU
	case profile.HeapProfile:
		sqlInsertSamples = sqlInsertSamplesHeap
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

func (st *pqStorage) Query(ctx context.Context, queryReq *profile.QueryRequest) ([]*profile.Profile, error) {
	if queryReq.Limit == 0 {
		queryReq.Limit = defaultProfilesLimit
	}

	ps, err := st.queryByCreatedAt(ctx, queryReq)
	if err != nil {
		err = fmt.Errorf("could not query profiles: %v", err)
	}
	return ps, err
}

func (st *pqStorage) queryByCreatedAt(ctx context.Context, queryReq *profile.QueryRequest) ([]*profile.Profile, error) {
	query := sqlSelectProfileByCreatedAt
	args := []interface{}{
		queryReq.Service,
		queryReq.Type,
		queryReq.CreatedAtMin,
		queryReq.CreatedAtMax,
	}

	var whereLabels string
	if len(queryReq.Labels) > 0 {
		for _, label := range queryReq.Labels {
			args = append(args, label.Value)
			whereLabels += fmt.Sprintf(" AND s.labels->'%s' = $%d", label.Key, len(args))
		}
	}
	query = fmt.Sprintf(query, whereLabels)

	if queryReq.Limit > 0 {
		args = append(args, queryReq.Limit)
		n := strconv.Itoa(len(args))
		if n == "" {
			return nil, fmt.Errorf("failed to build query with limit %d", queryReq.Limit)
		}
		query += " LIMIT $" + n
	}

	rows, err := st.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	panic("not implemented")

	//var (
	//	ps       []*profile.Profile
	//	hsLabels hstore.Hstore
	//)
	//for rows.Next() {
	//	var (
	//		p              profile.Profile
	//		buildID, token string
	//	)
	//	err = rows.Scan(&p.Digest, &p.Type, &p.Size, &p.CreatedAt, &p.ReceivedAt, &buildID, &token, &hsLabels)
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	p.Service = &profile.Service{
	//		BuildID: buildID,
	//		Token:   profile.TokenFromString(token),
	//		Labels:  hstoreToLabels(hsLabels, nil),
	//	}
	//
	//	ps = append(ps, &p)
	//}
	//if err := rows.Err(); err != nil {
	//	return nil, err
	//}
	//
	//return ps, nil
}

func (st *pqStorage) Delete(ctx context.Context, prof *profile.Profile) error {
	panic("implement me")
}

func createInsertSamples(table string, cols ...string) string {
	tCols := make([]string, 0, len(cols))
	for n, col := range cols {
		tCols = append(tCols, fmt.Sprintf("values_all[%d] AS %s", n+1, col))
	}
	return fmt.Sprintf(sqlInsertSamples, table, strings.Join(cols, ","), strings.Join(tCols, ","))
}
