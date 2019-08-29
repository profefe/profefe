package storage

import (
	"context"
	"io"
	"time"

	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/profile"
	"golang.org/x/xerrors"
)

var (
	ErrNotFound = xerrors.New("not found")
	ErrEmpty    = xerrors.New("empty results")
)

type Writer interface {
	WriteProfile(ctx context.Context, meta *profile.Meta, r io.Reader) error
}

type Reader interface {
	FindProfiles(ctx context.Context, params *FindProfilesParams) ([]*profile.Meta, error)
	ListProfiles(ctx context.Context, pid []profile.ID) (ProfileList, error)
}

type FindProfilesParams struct {
	Service      string
	Type         profile.ProfileType
	Labels       profile.Labels
	CreatedAtMin time.Time
	CreatedAtMax time.Time
	Limit        int
}

func (filter *FindProfilesParams) Validate() error {
	if filter == nil {
		return xerrors.New("nil request")
	}

	if filter.Service == "" {
		return xerrors.Errorf("service empty: filter %v", filter)
	}
	if filter.Type == profile.UnknownProfile {
		return xerrors.Errorf("unknown profile type %s: filter %v", filter.Type, filter)
	}
	if filter.CreatedAtMin.IsZero() || filter.CreatedAtMax.IsZero() {
		return xerrors.Errorf("createdAt time zero: filter %v", filter)
	}
	if filter.CreatedAtMin.After(filter.CreatedAtMax) {
		return xerrors.Errorf("createdAt time min after max: filter %v", filter)
	}
	return nil
}

type ProfileList interface {
	Next() (*pprofProfile.Profile, error)
	Close() error
}
