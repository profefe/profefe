package inmemory

import (
	"context"
	"fmt"
	"sync"

	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/store"
)

type repo struct {
	mu   sync.RWMutex
	repo map[string]*profile.Profile
}

func NewRepo() store.Repo {
	return &repo{
		repo: make(map[string]*profile.Profile),
	}
}

func (m *repo) Get(ctx context.Context, dgst string) (*profile.Profile, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if p, _ := m.repo[dgst]; p != nil {
		return p, nil
	}

	return nil, fmt.Errorf("no profile with dgst %s", dgst)
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

	return nil
}

func (m *repo) Delete(ctx context.Context, dgst string) error {
	panic("Delete: not implemented")
}

var poolProfiles = sync.Pool{
	New: func() interface{} { return new(profile.Profile) },
}

func (m *repo) Query(ctx context.Context, query store.RepoQuery) ([]*profile.Profile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	res := make([]*profile.Profile, 0, len(m.repo))

	pcopy := poolProfiles.Get().(*profile.Profile)
	defer poolProfiles.Put(pcopy)

	for _, p := range m.repo {
		*pcopy = *p
		if query(pcopy) {
			res = append(res, p)
			break
		}
	}

	return res, nil
}
