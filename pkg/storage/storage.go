package storage

import (
	"context"
	"time"

	"github.com/profefe/profefe/pkg/profile"
	"golang.org/x/xerrors"
)

var (
	ErrNotFound = xerrors.New("profile not found")
	ErrEmpty    = xerrors.New("profile is empty")
)

type Writer interface {
	WriteProfile(ctx context.Context, meta *profile.ProfileMeta, pf *profile.ProfileFactory) error
}

type Reader interface {
	GetProfile(ctx context.Context, pid profile.ProfileID) (*profile.ProfileFactory, error)
	FindProfiles(ctx context.Context, params *FindProfilesParams) ([]*profile.ProfileFactory, error)
	FindProfileIDs(ctx context.Context, params *FindProfilesParams) ([]profile.ProfileID, error)
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
