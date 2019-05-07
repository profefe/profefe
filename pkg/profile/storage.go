package profile

import (
	"context"
	"time"

	"github.com/profefe/profefe/internal/pprof/profile"
	"golang.org/x/xerrors"
)

var (
	ErrNotFound = xerrors.New("profile not found")
	ErrEmpty    = xerrors.New("profile is empty")
)

type GetServicesFilter struct {
	Service string
	Limit   uint
}

type GetProfileFilter struct {
	Service      string
	Type         ProfileType
	Labels       Labels
	CreatedAtMin time.Time
	CreatedAtMax time.Time
	Limit        uint
}

type Storage interface {
	CreateService(ctx context.Context, service *Service) error
	GetServices(ctx context.Context, filter *GetServicesFilter) ([]*Service, error)

	CreateProfile(ctx context.Context, prof *Profile, pp *profile.Profile) error
	GetProfile(ctx context.Context, filter *GetProfileFilter) (*profile.Profile, error)
	DeleteProfile(ctx context.Context, prof *Profile) error
}
