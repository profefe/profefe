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
	"github.com/profefe/profefe/pkg/storage"
)

type storageItem struct {
	profile *profile.Profile
	data    []byte
}

type inMemStorage struct {
	mu      sync.RWMutex
	storage map[profile.Digest]storageItem
}

func New() storage.Storage {
	return &inMemStorage{
		storage: make(map[profile.Digest]storageItem),
	}
}

func (s *inMemStorage) Create(ctx context.Context, meta map[string]interface{}, r io.Reader) (*profile.Profile, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
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

	s.storage[p.Digest] = storageItem{p, dataCopy}

	return p, nil
}

func (s *inMemStorage) Open(ctx context.Context, dgst profile.Digest) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	p, err := s.byDigest(ctx, dgst)
	if err != nil {
		return nil, err
	}

	return ioutil.NopCloser(bytes.NewReader(p.data)), nil
}

func (s *inMemStorage) Get(ctx context.Context, dgst profile.Digest) (*profile.Profile, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	p, err := s.byDigest(ctx, dgst)
	if err != nil {
		return nil, err
	}

	return p.profile, nil
}

func (s *inMemStorage) byDigest(_ context.Context, dgst profile.Digest) (p storageItem, err error) {
	p, ok := s.storage[dgst]
	if !ok {
		err = storage.ErrNotFound
	}
	return p, err
}

func (s *inMemStorage) List(ctx context.Context, filter func(*profile.Profile) bool) ([]*profile.Profile, error) {
	panic("implement me")
}

func (s *inMemStorage) Query(ctx context.Context, query *storage.QueryRequest) (ps []*profile.Profile, err error) {
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

func (s *inMemStorage) Delete(ctx context.Context, dgst profile.Digest) error {
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
