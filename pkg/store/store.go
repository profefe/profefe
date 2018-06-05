package store

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/profefe/profefe/pkg/profile"
)

const defaultDataRoot = "/tmp/profefe"

var (
	ErrNotFound = errors.New("not found")
)

type Store struct {
	repo Repo

	blobstore *fsBlobStore
}

func NewStore(repo Repo) (*Store, error) {
	s := &Store{
		repo: repo,
	}

	blobstore, err := newFsBlobStore(defaultDataRoot)
	if err != nil {
		return nil, fmt.Errorf("failed to create blobstore: %v", err)
	}
	s.blobstore = blobstore

	return s, nil
}

func (s *Store) Get(ctx context.Context, dgst string) (*profile.Profile, io.ReadCloser, error) {
	p, err := s.repo.Get(ctx, dgst)
	if err != nil {
		return nil, nil, fmt.Errorf("could not get profile: %v", err)
	}

	data, err := s.blobstore.Get(ctx, dgst)
	if err != nil {
		return nil, nil, err
	}

	return p, data, nil
}

func (s *Store) Create(ctx context.Context, meta map[string]string, data []byte) (*profile.Profile, error) {
	dgst, size, err := s.blobstore.Put(ctx, data)
	if err != nil {
		return nil, err
	}

	p := profile.NewWithMeta(meta)
	p.Digest = dgst
	p.Size = size

	if err := s.repo.Create(ctx, p); err != nil {
		return nil, fmt.Errorf("could not create profile: %v", err)
	}

	return p, nil
}

func (s *Store) Find(ctx context.Context, meta map[string]string) (*profile.Profile, io.ReadCloser, error) {
	p := profile.NewWithMeta(meta)
	if p.Service == "" {
		return nil, nil, fmt.Errorf("profile without service")
	}

	labelsCache := make(map[string]string, len(p.Labels))
	for _, label := range p.Labels {
		if label.Key != "" {
			labelsCache[label.Key] = label.Value
		}
	}

	pp, err := s.repo.Query(ctx, func(pq *profile.Profile) bool {
		found := pq.Service == p.Service
		if !found || len(labelsCache) == 0 {
			return found
		}

		for _, label := range pq.Labels {
			if _, ok := labelsCache[label.Key]; !ok {
				return false
			}
		}

		return true
	})
	if err != nil {
		return nil, nil, fmt.Errorf("could not find profile: %v", err)
	}
	if len(pp) == 0 {
		return nil, nil, ErrNotFound
	}

	// XXX return first profile only for now
	p = pp[0]
	data, err := s.blobstore.Get(ctx, p.Digest)
	if err != nil {
		return nil, nil, err
	}
	return p, data, nil
}
