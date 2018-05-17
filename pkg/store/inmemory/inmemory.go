package inmemory

import (
	"context"
	"fmt"
	"sync"

	"github.com/profefe/profefe/pkg/store"
)

type repo struct {
	mu          sync.Mutex
	repo        map[string]*store.Profile
	byNameIndex map[string][]string
}

func NewRepo() store.Repo {
	return &repo{
		repo:        make(map[string]*store.Profile),
		byNameIndex: make(map[string][]string),
	}
}

func (m *repo) Get(ctx context.Context, dgst string) (*store.Profile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if p, _ := m.repo[dgst]; p != nil {
		return p, nil
	}

	return nil, fmt.Errorf("no profile with dgst %s", dgst)
}

func (m *repo) Create(ctx context.Context, p *store.Profile) error {
	if p.Digest == "" {
		return fmt.Errorf("bad profile: %v", p)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.repo[p.Digest]; ok {
		return fmt.Errorf("duplicate profile: %v", p)
	}

	m.repo[p.Digest] = p
	if p.Name != "" {
		m.byNameIndex[p.Name] = append(m.byNameIndex[p.Name], p.Digest)
	}

	return nil
}

var poolProfiles = sync.Pool{
	New: func() interface{} { return new(store.Profile) },
}

func (m *repo) ByName(ctx context.Context, name string, queries ...store.RepoQuery) ([]*store.Profile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	hashes, ok := m.byNameIndex[name]
	if !ok {
		return nil, fmt.Errorf("no profile with name %s", name)
	}

	res := make([]*store.Profile, len(hashes))

	pcopy := poolProfiles.Get().(*store.Profile)
	defer poolProfiles.Put(pcopy)

	for i, hash := range hashes {
		p := m.repo[hash]
		if len(queries) == 0 {
			res[i] = p
			continue
		}
		for _, query := range queries {
			*pcopy = *p
			if query(pcopy) {
				res[i] = p
			}
		}
	}

	return res, nil
}
