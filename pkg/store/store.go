package store

import (
	"context"
	"fmt"
	"io"
	"time"
)

const dataRoot = "/tmp/profefe"

type Store struct {
	metastore *metaStore
	blobstore *fsBlobStore
}

func NewStore() (*Store, error) {
	blobstore, err := newFsBlobStore(dataRoot)
	if err != nil {
		return nil, fmt.Errorf("failed to create blob store: %v", err)
	}

	s := &Store{
		metastore: newMetaStore(),
		blobstore: blobstore,
	}
	return s, nil
}

func (s *Store) Get(ctx context.Context, hash string) (*Profile, io.ReadCloser, error) {
	p, err := s.metastore.Get(hash)
	if err != nil {
		return nil, nil, fmt.Errorf("could not get profile: %v", err)
	}

	data, err := s.blobstore.Get(ctx, hash)
	if err != nil {
		return nil, nil, fmt.Errorf("could not get profile data from blobstore: %v", err)
	}
	return p, data, nil
}

func (s *Store) Save(ctx context.Context, meta map[string]string, data []byte) (*Profile, error) {
	p := &Profile{
		Type:       UnknownProfile,
		ReceivedAt: time.Now().UTC(),
	}
	parseProfileMeta(p, meta)

	hash, size, err := s.blobstore.Put(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("could not save data to blobstore: %v", err)
	}

	p.Hash = hash
	p.Size = size

	if err := s.metastore.Put(ctx, p); err != nil {
		return nil, fmt.Errorf("could not save profile: %v", err)
	}

	return p, nil
}
