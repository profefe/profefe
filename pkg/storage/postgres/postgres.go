package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strconv"

	pprof "github.com/google/pprof/profile"
	"github.com/lib/pq/hstore"
	"github.com/profefe/profefe/pkg/filestore"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
)

const (
	queryInsertServiceOnce = `
		INSERT INTO services(build_id, generation, name, labels)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (build_id, generation) DO NOTHING`
	queryInsertProfile = `
		INSERT INTO profiles_pprof(digest, type, size, created_at, received_at, build_id, generation)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`
	querySelectByCreatedAt = `
		SELECT
			p.digest,
			p.size,
			p.created_at,
			p.received_at,
			p.build_id,
			p.generation,
			s.labels
		FROM profiles_pprof p, services s
		WHERE p.build_id = s.build_id AND p.generation = s.generation 
		AND s.name = $1 AND p.type = $2
		%s
		AND p.created_at BETWEEN $3 AND $4
		ORDER BY p.created_at`
)

const defaultProfilesLimit = 100

type pqStorage struct {
	db *sql.DB
	fs *filestore.FileStore
}

func New(db *sql.DB, fileStore *filestore.FileStore) (storage.Storage, error) {
	s := &pqStorage{
		db: db,
		fs: fileStore,
	}
	return s, nil
}

func (s *pqStorage) Create(ctx context.Context, meta map[string]interface{}, r io.Reader) (*profile.Profile, error) {
	dgst, size, data, err := s.fs.Save(ctx, r)
	if err != nil {
		return nil, err
	}

	prof, err := pprof.ParseData(data)
	if err != nil {
		return nil, fmt.Errorf("could not parse profile from reader: %v", err)
	}

	p := profile.NewWithMeta(prof, meta)
	p.Digest = dgst
	p.Size = size

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() {
		var txErr error
		if err == nil {
			txErr = tx.Commit()
		} else {
			txErr = tx.Rollback()
		}
		if txErr != nil {
			err = txErr
		}
	}()

	_, err = tx.ExecContext(ctx, queryInsertServiceOnce, p.BuildID, p.Generation, p.Service, hstoreFromLabels(p.Labels))
	if err != nil {
		return nil, fmt.Errorf("could not upsert service: %v", err)
	}

	_, err = tx.ExecContext(ctx, queryInsertProfile, p.Digest, p.Type, p.Size, p.CreatedAt, p.ReceivedAt, p.BuildID, p.Generation)
	if err != nil {
		return nil, fmt.Errorf("could not insert profile: %v", err)
	}

	return p, err
}

func (s *pqStorage) Open(ctx context.Context, dgst profile.Digest) (io.ReadCloser, error) {
	return s.fs.Get(ctx, dgst)
}

func (s *pqStorage) Get(ctx context.Context, dgst profile.Digest) (*profile.Profile, error) {
	panic("implement me")
}

func (s *pqStorage) Query(ctx context.Context, queryReq *storage.QueryRequest) ([]*profile.Profile, error) {
	if queryReq.Limit == 0 {
		queryReq.Limit = defaultProfilesLimit
	}

	ps, err := s.queryByCreatedAt(ctx, queryReq)
	if err != nil {
		err = fmt.Errorf("could not select profiles: %v", err)
	}
	return ps, err
}

func (s *pqStorage) queryByCreatedAt(ctx context.Context, queryReq *storage.QueryRequest) ([]*profile.Profile, error) {
	query := querySelectByCreatedAt
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

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		ps       []*profile.Profile
		hsLabels hstore.Hstore
	)

	for rows.Next() {
		var p profile.Profile
		err = rows.Scan(&p.Digest, &p.Size, &p.CreatedAt, &p.ReceivedAt, &p.BuildID, &p.Generation, &hsLabels)
		if err != nil {
			return nil, err
		}

		p.Labels = hstoreToLabes(hsLabels, p.Labels)

		ps = append(ps, &p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return ps, nil
}

func (s *pqStorage) Delete(ctx context.Context, dgst profile.Digest) error {
	panic("implement me")
}
