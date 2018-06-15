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

type repo struct {
	mu      sync.RWMutex
	storage map[profile.Digest]repoItem
}

func New() profile.Repo {
	return &repo{
		storage: make(map[profile.Digest]repoItem),
	}
}

func (r *repo) Create(ctx context.Context, meta map[string]interface{}, data []byte) (*profile.Profile, error) {
	dgst, err := getDigestFor(data)
	if err != nil {
		return nil, err
	}

	p := profile.NewWithMeta(meta)
	p.Digest = dgst
	p.Size = int64(len(data))

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.storage[p.Digest]; ok {
		return nil, fmt.Errorf("duplicate profile: %v", p)
	}

	r.storage[p.Digest] = repoItem{p, dataCopy}

	return p, nil
}

func (r *repo) Open(ctx context.Context, dgst profile.Digest) (io.ReadCloser, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	p, err := r.byDigest(ctx, dgst)
	if err != nil {
		return nil, err
	}

	return ioutil.NopCloser(bytes.NewReader(p.data)), nil
}

func (r *repo) Get(ctx context.Context, dgst profile.Digest) (*profile.Profile, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	p, err := r.byDigest(ctx, dgst)
	if err != nil {
		return nil, err
	}

	return p.profile, nil
}

func (r *repo) byDigest(_ context.Context, dgst profile.Digest) (p repoItem, err error) {
	p, ok := r.storage[dgst]
	if !ok {
		err = store.ErrNotFound
	}
	return p, err
}

func (r *repo) List(ctx context.Context, filter func(*profile.Profile) bool) ([]*profile.Profile, error) {
	panic("implement me")
}

func (r *repo) Query(ctx context.Context, query *profile.QueryRequest) (ps []*profile.Profile, err error) {
	for _, pit := range r.storage {
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

func (r *repo) Delete(ctx context.Context, dgst profile.Digest) error {
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
