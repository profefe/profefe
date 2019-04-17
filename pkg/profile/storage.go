package profile

import (
	"context"
	"errors"
	"time"

	"github.com/profefe/profefe/internal/pprof/profile"
)

var (
	ErrNotFound = errors.New("profile not found")
	ErrEmpty    = errors.New("profile is empty")
)

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

	CreateProfile(ctx context.Context, prof *Profile, pp *profile.Profile) error
	GetProfile(ctx context.Context, filter *GetProfileFilter) (*profile.Profile, error)
	DeleteProfile(ctx context.Context, prof *Profile) error
}
