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
	WriteProfile(ctx context.Context, meta profile.Meta, r io.Reader) error
}

type Reader interface {
	FindProfiles(ctx context.Context, params *FindProfilesParams) ([]profile.Meta, error)
	FindProfileIDs(ctx context.Context, params *FindProfilesParams) ([]profile.ID, error)
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

func (params *FindProfilesParams) Validate() error {
	if params == nil {
		return xerrors.New("nil request")
	}
	if params.Service == "" {
		return xerrors.New("service empty")
	}
	if params.Type == profile.UnknownProfile {
		return xerrors.Errorf("unknown profile type %s", params.Type)
	}
	if params.CreatedAtMin.IsZero() || params.CreatedAtMax.IsZero() {
		return xerrors.Errorf("profile created time zero: min %v, max %v", params.CreatedAtMin, params.CreatedAtMax)
	}
	if params.CreatedAtMin.After(params.CreatedAtMax) {
		return xerrors.Errorf("profile created time min after max: min %v, max %v", params.CreatedAtMin, params.CreatedAtMax)
	}
	return nil
}

type ProfileList interface {
	Next() bool
	Profile() (*pprofProfile.Profile, error)
	Close() error
}
