package store

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/profefe/profefe/pkg/profile"
)

var (
	ErrNotFound = errors.New("not found")
)

type Store struct {
	repo profile.Repo

	Log func(string, ...interface{})
}

func New(repo profile.Repo) *Store {
	return &Store{
		repo: repo,
		Log:  func(_ string, _ ...interface{}) {},
	}
}

func (s *Store) Create(ctx context.Context, meta map[string]interface{}, data []byte) (*profile.Profile, error) {
	p, err := s.repo.Create(ctx, meta, data)
	if err != nil {
		return nil, err
	}
	s.Log("create: new profile %v", p)
	return p, nil
}

func validateQueryRequest(q *profile.QueryRequest) error {
	if q == nil {
		return errors.New("nil query request")
	}

	if q.Digest != "" {
		return nil
	}

	if q.Service == "" {
		return fmt.Errorf("no service: query %v", q)
	}
	if q.Type == profile.UnknownProfile {
		return fmt.Errorf("unknown profile type %s: query %v", q.Type, q)
	}
	if q.CreatedAtMin.IsZero() || q.CreatedAtMax.IsZero() {
		return fmt.Errorf("createdAt time zero: query %v", q)
	}
	if q.CreatedAtMax.Before(q.CreatedAtMin) {
		return fmt.Errorf("createdAt time min after max: query %v", q)
	}
	return nil
}

func (s *Store) Lookup(ctx context.Context, query *profile.QueryRequest) (*profile.Profile, io.ReadCloser, error) {
	if err := validateQueryRequest(query); err != nil {
		return nil, nil, err
	}

	ps, err := s.repo.Query(ctx, query)
	if err != nil {
		return nil, nil, err
	}

	if len(ps) == 0 {
		return nil, nil, ErrNotFound
	} else if len(ps) > 1 {
		s.Log("lookup: found %d profiles by query %v", len(ps), query)
	}

	p := ps[0]
	pr, err := s.repo.Open(ctx, p.Digest)
	if err != nil {
		return nil, nil, fmt.Errorf("could not open profile %s: %v", p.Digest, err)
	}

	return p, pr, err
}
