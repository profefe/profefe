package inmemory

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"sync"

	"github.com/profefe/profefe/pkg/profile"
)

type storageKey struct {
	buildID string
	token   profile.Token
}

type storageItem struct {
	profiles  []*profile.Profile
	labelsSet map[profile.Label]struct{}
}

type Storage struct {
	mu      sync.RWMutex
	storage map[storageKey]storageItem
}

var _ profile.Storage = (*Storage)(nil)

func New() *Storage {
	return &Storage{
		storage: make(map[storageKey]storageItem),
	}
}

func (st *Storage) Create(ctx context.Context, prof *profile.Profile) error {
	return nil
}

func (st *Storage) Update(ctx context.Context, prof *profile.Profile, r io.Reader) error {
	return nil
}

func (st *Storage) Open(ctx context.Context, dgst profile.Digest) (io.ReadCloser, error) {
	return nil, nil
}

func (st *Storage) Get(ctx context.Context, dgst profile.Digest) (*profile.Profile, error) {
	return nil, nil
}

func (st *Storage) byDigest(_ context.Context, dgst profile.Digest) (p storageItem, err error) {
	p, ok := st.storage[dgst]
	if !ok {
		err = profile.ErrNotFound
	}
	return p, err
}

func (st *Storage) Query(ctx context.Context, query *profile.QueryRequest) (profs []*profile.Profile, err error) {
	if query == nil {
		return nil, profile.ErrNotFound
	}

	st.mu.RLock()
	defer st.mu.RUnlock()

	for _, item := range st.storage {
		prof := item.profile
		if query.Digest != "" && prof.Digest != query.Digest {
			continue
		}
		if query.Service != "" && prof.Service.Name != query.Service {
			continue
		}
		if query.Type != profile.UnknownProfile && prof.Type != query.Type {
			continue
		}
		if !query.CreatedAtMin.IsZero() && prof.CreatedAt.Before(query.CreatedAtMin) {
			continue
		}
		if !query.CreatedAtMax.IsZero() && prof.CreatedAt.After(query.CreatedAtMax) {
			continue
		}

		ok := true
		for _, label := range query.Labels {
			if _, ok = item.labelsSet[label]; !ok {
				break
			}
		}
		if !ok {
			continue
		}

		profs = append(profs, prof)
	}

	if len(profs) == 0 {
		return nil, profile.ErrNotFound
	}

	return profs, nil
}

func (st *Storage) Delete(ctx context.Context, dgst profile.Digest) error {
	st.mu.Lock()
	delete(st.storage, dgst)
	st.mu.Unlock()
	return nil
}

func (st *Storage) getDigestFor(data []byte) (profile.Digest, error) {
	h := sha1.New()
	if _, err := h.Write(data); err != nil {
		return "", err
	}
	dgstStr := hex.EncodeToString(h.Sum(nil))
	return profile.Digest(dgstStr), nil
}
