package inmemory

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"sync"

	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/store"
)

type repoItem struct {
	profile *profile.Profile
	data    []byte
}

type inMemStore struct {
	mu      sync.RWMutex
	storage map[profile.Digest]repoItem
}

func New() store.Store {
	return &inMemStore{
		storage: make(map[profile.Digest]repoItem),
	}
}

func (s *inMemStore) Create(ctx context.Context, meta map[string]interface{}, data []byte) (*profile.Profile, error) {
	dgst, err := getDigestFor(data)
	if err != nil {
		return nil, err
	}

	p := profile.NewWithMeta(meta)
	p.Digest = dgst
	p.Size = int64(len(data))

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.storage[p.Digest]; ok {
		return nil, fmt.Errorf("duplicate profile: %v", p)
	}

	s.storage[p.Digest] = repoItem{p, dataCopy}

	return p, nil
}

func (s *inMemStore) Open(ctx context.Context, dgst profile.Digest) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	p, err := s.byDigest(ctx, dgst)
	if err != nil {
		return nil, err
	}

	return ioutil.NopCloser(bytes.NewReader(p.data)), nil
}

func (s *inMemStore) Get(ctx context.Context, dgst profile.Digest) (*profile.Profile, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	p, err := s.byDigest(ctx, dgst)
	if err != nil {
		return nil, err
	}

	return p.profile, nil
}

func (s *inMemStore) byDigest(_ context.Context, dgst profile.Digest) (p repoItem, err error) {
	p, ok := s.storage[dgst]
	if !ok {
		err = store.ErrNotFound
	}
	return p, err
}

func (s *inMemStore) List(ctx context.Context, filter func(*profile.Profile) bool) ([]*profile.Profile, error) {
	panic("implement me")
}

func (s *inMemStore) Query(ctx context.Context, query *store.QueryRequest) (ps []*profile.Profile, err error) {
	for _, pit := range s.storage {
		p := pit.profile
		if p.Service != query.Service {
			continue
		}
		if p.Type != query.Type {
			continue
		}
		if p.CreatedAt.Before(query.CreatedAtMin) || p.CreatedAt.After(query.CreatedAtMax) {
			continue
		}
		ps = append(ps, p)
	}
	return ps, nil
}

func (s *inMemStore) Delete(ctx context.Context, dgst profile.Digest) error {
	panic("implement me")
}

func getDigestFor(data []byte) (profile.Digest, error) {
	h := sha1.New()
	if _, err := h.Write(data); err != nil {
		return "", err
	}
	dgstStr := hex.EncodeToString(h.Sum(nil))
	return profile.Digest(dgstStr), nil
}
