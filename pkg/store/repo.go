package store

import (
	"context"

	"github.com/profefe/profefe/pkg/profile"
)

type Repo interface {
	Get(ctx context.Context, dgst string) (*profile.Profile, error)
	Create(ctx context.Context, p *profile.Profile) error

	ByService(ctx context.Context, service string, queries ...RepoQuery) ([]*profile.Profile, error)
}

type RepoQuery func(p *profile.Profile) bool
