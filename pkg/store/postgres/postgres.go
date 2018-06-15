package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"io"

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
		INSERT INTO profiles(digest, type, size, created_at, received_at, build_id, generation) 
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
		FROM profiles AS p, services AS s
		WHERE p.build_id = s.build_id AND p.generation = s.generation AND s.name = $1 AND p.type = $2 
		AND p.created_at BETWEEN $3 AND $4
		LIMIT $5`
	querySelectByLabels = ``
)

const defaultProfilesLimit = 100

type pqRepo struct {
	db        *sql.DB
	fileStore *filestore.FileStore
}

func New(db *sql.DB, fileStore *filestore.FileStore) (profile.Repo, error) {
	s := &pqRepo{
		db:        db,
		fileStore: fileStore,
	}
	return s, nil
}

func (r *pqRepo) Create(ctx context.Context, meta map[string]interface{}, data []byte) (*profile.Profile, error) {
	dgst, size, err := r.fileStore.Put(ctx, data)
	if err != nil {
		return nil, err
	}

	p := profile.NewWithMeta(meta)
	p.Digest = dgst
	p.Size = size

	tx, err := r.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			err = tx.Rollback()
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

func (r *pqRepo) Open(ctx context.Context, dgst profile.Digest) (io.ReadCloser, error) {
	return r.fileStore.Get(ctx, dgst)
}

func (r *pqRepo) Get(ctx context.Context, dgst profile.Digest) (*profile.Profile, error) {
	panic("implement me")
}

func (r *pqRepo) List(ctx context.Context, filter func(*profile.Profile) bool) ([]*profile.Profile, error) {
	panic("implement me")
}

func (r *pqRepo) Query(ctx context.Context, queryReq *profile.QueryRequest) ([]*profile.Profile, error) {
	if queryReq.Limit == 0 {
		queryReq.Limit = defaultProfilesLimit
	}

	ps, err := r.queryByCreatedAt(ctx, queryReq)
	if err != nil {
		err = fmt.Errorf("could not select profiles: %v", err)
	}
	return ps, err
}

func (r *pqRepo) queryByCreatedAt(ctx context.Context, queryReq *profile.QueryRequest) ([]*profile.Profile, error) {
	rows, err := r.db.QueryContext(
		ctx,
		querySelectByCreatedAt,
		queryReq.Service,
		queryReq.Type,
		queryReq.CreatedAtMin,
		queryReq.CreatedAtMax,
		queryReq.Limit,
	)
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

func (r *pqRepo) Delete(ctx context.Context, dgst profile.Digest) error {
	panic("implement me")
}
