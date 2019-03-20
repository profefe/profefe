package profile

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/profefe/profefe/internal/pprof/profile"
)

var (
	ErrNotFound = errors.New("profile not found")
	ErrEmpty    = errors.New("profile is empty")
)

type ReadProfileFilter struct {
	Service      string
	Type         ProfileType
	Labels       Labels
	CreatedAtMin time.Time
	CreatedAtMax time.Time
	Limit        uint
}

type Storage interface {
	CreateService(ctx context.Context, service *Service) error

	CreateProfile(ctx context.Context, prof *Profile, r io.Reader) error
	ReadProfile(ctx context.Context, filter *ReadProfileFilter) (io.Reader, error)
	ReadRawProfile(ctx context.Context, filter *ReadProfileFilter) (*profile.Profile, error)
	DeleteProfile(ctx context.Context, prof *Profile) error
}
