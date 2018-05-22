package store

import (
	"context"
	"fmt"
	"io"

	"github.com/profefe/profefe/pkg/profile"
)

const defaultDataRoot = "/tmp/profefe"

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

func (s *Store) Save(ctx context.Context, meta map[string]string, data []byte) (*profile.Profile, error) {
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
