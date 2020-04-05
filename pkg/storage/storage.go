package storage

import (
	"context"
	"io"
	"time"

	"github.com/profefe/profefe/pkg/profile"
	"golang.org/x/xerrors"
)

var (
	ErrNotFound = xerrors.New("not found")
	ErrEmpty    = xerrors.New("empty results")
)

type Storage interface {
	Writer
	Reader
}

type Writer interface {
	WriteProfile(ctx context.Context, params *WriteProfileParams, r io.Reader) (profile.Meta, error)
}

type WriteProfileParams struct {
	Service   string
	Type      profile.ProfileType
	Labels    profile.Labels
	CreatedAt time.Time
}

func (params *WriteProfileParams) Validate() error {
	if params == nil {
		return xerrors.New("empty params")
	}
	if params.Service == "" {
		return xerrors.New("empty service")
	}
	if params.Type == profile.TypeUnknown {
		return xerrors.Errorf("unknown profile type %s", params.Type)
	}
	return nil
}

type Reader interface {
	FindProfiles(ctx context.Context, params *FindProfilesParams) ([]profile.Meta, error)
	FindProfileIDs(ctx context.Context, params *FindProfilesParams) ([]profile.ID, error)
	ListProfiles(ctx context.Context, pid []profile.ID) (ProfileList, error)
	ListServices(ctx context.Context) ([]string, error)
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
		return xerrors.New("empty params")
	}
	if params.Service == "" {
		return xerrors.New("service empty")
	}
	if params.Type == profile.TypeUnknown {
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
	Profile() (io.Reader, error)
	Close() error
}
