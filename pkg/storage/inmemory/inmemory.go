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

	"github.com/profefe/profefe/pkg/collector"
	"github.com/profefe/profefe/pkg/profile"
)

type storageItem struct {
	profile   *profile.Profile
	labelsIdx map[profile.Label]struct{}
	data      []byte
}

type Storage struct {
	mu      sync.RWMutex
	storage map[profile.Digest]storageItem
}

var _ profile.Storage = (*Storage)(nil)

func New() *Storage {
	return &Storage{
		storage: make(map[profile.Digest]storageItem),
	}
}

func (s *Storage) Create(ctx context.Context, p *profile.Profile, r io.Reader) error {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	dgst, err := s.getDigestFor(data)
	if err != nil {
		return err
	}

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	s.mu.Lock()
	defer s.mu.Unlock()

	p.Digest = dgst
	p.Size = int64(len(data))

	if _, ok := s.storage[p.Digest]; ok {
		return fmt.Errorf("duplicate profile: %v", p)
	}

	labelsIdx := make(map[profile.Label]struct{})
	for _, label := range p.Labels {
		labelsIdx[label] = struct{}{}
	}

	s.storage[p.Digest] = storageItem{p, labelsIdx, dataCopy}

	return nil
}

func (s *Storage) Open(ctx context.Context, dgst profile.Digest) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	p, err := s.byDigest(ctx, dgst)
	if err != nil {
		return nil, err
	}

	return ioutil.NopCloser(bytes.NewReader(p.data)), nil
}

func (s *Storage) Get(ctx context.Context, dgst profile.Digest) (*profile.Profile, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	p, err := s.byDigest(ctx, dgst)
	if err != nil {
		return nil, err
	}

	return p.profile, nil
}

func (s *Storage) byDigest(_ context.Context, dgst profile.Digest) (p storageItem, err error) {
	p, ok := s.storage[dgst]
	if !ok {
		err = collector.ErrNotFound
	}
	return p, err
}

func (s *Storage) Query(ctx context.Context, query *profile.QueryRequest) (ps []*profile.Profile, err error) {
	if query == nil {
		return nil, collector.ErrNotFound
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, item := range s.storage {
		p := item.profile
		if query.Digest != "" && p.Digest != query.Digest {
			continue
		}
		if query.Service != "" && p.Service != query.Service {
			continue
		}
		if query.Type != profile.UnknownProfile && p.Type != query.Type {
			continue
		}
		if !query.CreatedAtMin.IsZero() && p.CreatedAt.Before(query.CreatedAtMin) {
			continue
		}
		if !query.CreatedAtMax.IsZero() && p.CreatedAt.After(query.CreatedAtMax) {
			continue
		}

		ok := true
		for _, label := range query.Labels {
			if _, ok = item.labelsIdx[label]; !ok {
				break
			}
		}
		if !ok {
			continue
		}

		ps = append(ps, p)
	}

	if len(ps) == 0 {
		return nil, collector.ErrNotFound
	}

	return ps, nil
}

func (s *Storage) Delete(ctx context.Context, dgst profile.Digest) error {
	s.mu.Lock()
	delete(s.storage, dgst)
	s.mu.Unlock()

	return nil
}

func (s *Storage) getDigestFor(data []byte) (profile.Digest, error) {
	h := sha1.New()
	if _, err := h.Write(data); err != nil {
		return "", err
	}
	dgstStr := hex.EncodeToString(h.Sum(nil))
	return profile.Digest(dgstStr), nil
}
