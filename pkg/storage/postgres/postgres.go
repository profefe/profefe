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
	queryInsertService = `
		INSERT INTO services(name, build_id, token, created_at, labels)
		VALUES ($1, $2, $3, $4, $5)`
	queryInsertProfile = `
		INSERT INTO profiles_pprof(digest, type, size, build_id, token, created_at, received_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`
	querySelectProfileByCreatedAt = `
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

func (st *pqStorage) Create(ctx context.Context, p *profile.Profile) error {
	_, err := st.db.ExecContext(
		ctx,
		queryInsertService,
		p.Service.Name,
		p.Service.BuildID,
		p.Service.Token.String(),
		p.CreatedAt,
		hstoreFromLabels(p.Service.Labels),
	)
	if err != nil {
		err = fmt.Errorf("pg: could not insert %+v into services: %v", p, err)
	}
	return err
}

func (st *pqStorage) Update(ctx context.Context, p *profile.Profile, r io.Reader) error {
	dgst, size, err := st.fs.Save(ctx, r)
	if err != nil {
		return err
	}
	if size == 0 {
		return profile.ErrEmpty
	}

	_, err = st.db.ExecContext(
		ctx,
		queryInsertProfile,
		dgst,
		p.Type,
		size,
		p.Service.BuildID,
		p.Service.Token.String(),
		p.CreatedAt,
		p.ReceivedAt,
	)
	if err != nil {
		return fmt.Errorf("pg: could not insert %+v into profiles: %v", p, err)
	}

	return nil
}

func (st *pqStorage) Open(ctx context.Context, dgst profile.Digest) (io.ReadCloser, error) {
	return st.fs.Get(ctx, dgst)
}

func (st *pqStorage) Get(ctx context.Context, dgst profile.Digest) (*profile.Profile, error) {
	panic("implement me")
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
	query := querySelectProfileByCreatedAt
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

	var (
		ps       []*profile.Profile
		hsLabels hstore.Hstore
	)
	for rows.Next() {
		var (
			p              profile.Profile
			buildID, token string
		)
		err = rows.Scan(&p.Digest, &p.Type, &p.Size, &p.CreatedAt, &p.ReceivedAt, &buildID, &token, &hsLabels)
		if err != nil {
			return nil, err
		}

		p.Service = &profile.Service{
			BuildID: buildID,
			Token:   profile.TokenFromString(token),
			Labels:  hstoreToLabels(hsLabels, nil),
		}

		ps = append(ps, &p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return ps, nil
}

func (st *pqStorage) Delete(ctx context.Context, dgst profile.Digest) error {
	panic("implement me")
}
