package inmemory

import (
	"context"
	"fmt"
	"sync"

	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/store"
)

type repo struct {
	mu             sync.Mutex
	repo           map[string]*profile.Profile
	byServiceIndex map[string][]string
}

func NewRepo() store.Repo {
	return &repo{
		repo:           make(map[string]*profile.Profile),
		byServiceIndex: make(map[string][]string),
	}
}

func (m *repo) Create(ctx context.Context, p *profile.Profile) error {
	if p.Digest == "" {
		return fmt.Errorf("bad profile: %v", p)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.repo[p.Digest]; ok {
		return fmt.Errorf("duplicate profile: %v", p)
	}

	m.repo[p.Digest] = p
	if p.Service != "" {
		m.byServiceIndex[p.Service] = append(m.byServiceIndex[p.Service], p.Digest)
	}

	return nil
}

func (m *repo) Get(ctx context.Context, dgst string) (*profile.Profile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if p, _ := m.repo[dgst]; p != nil {
		return p, nil
	}

	return nil, fmt.Errorf("no profile with dgst %s", dgst)
}

var poolProfiles = sync.Pool{
	New: func() interface{} { return new(profile.Profile) },
}

func (m *repo) ByService(ctx context.Context, service string, queries ...store.RepoQuery) ([]*profile.Profile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	hashes, ok := m.byServiceIndex[service]
	if !ok {
		return nil, fmt.Errorf("no profile for service %s", service)
	}

	res := make([]*profile.Profile, len(hashes))

	pcopy := poolProfiles.Get().(*profile.Profile)
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
