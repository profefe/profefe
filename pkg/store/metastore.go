package store

import (
	"context"
	"fmt"
	"sync"
)

type metaStore struct {
	mu    sync.Mutex
	data  map[string]*Profile
	index map[string][]string
}

func newMetaStore() *metaStore {
	return &metaStore{
		data:  make(map[string]*Profile),
		index: make(map[string][]string),
	}
}

func (m *metaStore) Get(hash string) (*Profile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if p, _ := m.data[hash]; p != nil {
		return p, nil
	}

	return nil, fmt.Errorf("no profile with hash %s", hash)
}

func (m *metaStore) Put(ctx context.Context, p *Profile) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.data[p.Hash]; ok {
		return fmt.Errorf("duplicate profile")
	}

	m.data[p.Hash] = p
	m.index[p.Name] = append(m.index[p.Name], p.Hash)

	return nil
}

func (m *metaStore) ByName(name string) ([]*Profile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	hashes, ok := m.index[name]
	if !ok {
		return nil, nil
	}

	res := make([]*Profile, len(hashes))
	for i, hash := range hashes {
		res[i] = m.data[hash]
	}

	return res, nil
}
