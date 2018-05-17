package store

import "context"

type Repo interface {
	Get(ctx context.Context, hash string) (*Profile, error)
	Create(ctx context.Context, p *Profile) error

	ByName(ctx context.Context, name string, queries ...RepoQuery) ([]*Profile, error)
}

type RepoQuery func(p *Profile) bool
