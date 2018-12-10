package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strconv"

	"github.com/lib/pq/hstore"
	"github.com/profefe/profefe/pkg/filestore"
	"github.com/profefe/profefe/pkg/profile"
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

func New(db *sql.DB, fs *filestore.FileStore) (profile.Storage, error) {
	s := &pqStorage{
		db: db,
		fs: fs,
	}
	return s, nil
}

func (s *pqStorage) Create(ctx context.Context, p *profile.Profile, r io.Reader) error {
	dgst, size, err := s.fs.Save(ctx, r)
	if err != nil {
		return err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
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
		return fmt.Errorf("could not upsert service: %v", err)
	}

	_, err = tx.ExecContext(ctx, queryInsertProfile, dgst, p.Type, size, p.CreatedAt, p.ReceivedAt, p.BuildID, p.Generation)
	if err != nil {
		return fmt.Errorf("could not insert profile: %v", err)
	}

	// TODO(narqo): updating profile inside the Create seems smelly; needs to think more about the API
	p.Digest = dgst
	p.Size = size

	return nil
}

func (s *pqStorage) Open(ctx context.Context, dgst profile.Digest) (io.ReadCloser, error) {
	return s.fs.Get(ctx, dgst)
}

func (s *pqStorage) Get(ctx context.Context, dgst profile.Digest) (*profile.Profile, error) {
	panic("implement me")
}

func (s *pqStorage) Query(ctx context.Context, queryReq *profile.QueryRequest) ([]*profile.Profile, error) {
	if queryReq.Limit == 0 {
		queryReq.Limit = defaultProfilesLimit
	}

	ps, err := s.queryByCreatedAt(ctx, queryReq)
	if err != nil {
		err = fmt.Errorf("could not query profiles: %v", err)
	}
	return ps, err
}

func (s *pqStorage) queryByCreatedAt(ctx context.Context, queryReq *profile.QueryRequest) ([]*profile.Profile, error) {
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
