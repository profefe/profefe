package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/profefe/profefe/pkg/profile"
)

var (
	ErrNotFound       = errors.New("not found")
	ErrNoResults      = errors.New("no results")
	ErrNotImplemented = errors.New("method not implemented")
)

type Storage interface {
	Writer
	Reader
}

type Writer interface {
	WriteProfile(ctx context.Context, params *WriteProfileParams, r io.Reader) (profile.Meta, error)
}

type WriteProfileParams struct {
	ExternalID profile.ID
	Service    string
	Type       profile.ProfileType
	Labels     profile.Labels
	CreatedAt  time.Time
}

func (params *WriteProfileParams) Validate() error {
	if params == nil {
		return errors.New("empty params")
	}
	if params.Service == "" {
		return errors.New("empty service")
	}
	if params.Type == profile.TypeUnknown {
		return fmt.Errorf("unknown profile type %s", params.Type)
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
		return errors.New("empty params")
	}
	if params.Service == "" {
		return errors.New("empty service")
	}
	if params.Type == profile.TypeUnknown {
		return fmt.Errorf("unknown profile type %s", params.Type)
	}
	if params.CreatedAtMin.IsZero() || params.CreatedAtMax.IsZero() {
		return fmt.Errorf("created_at is zero: min %v, max %v", params.CreatedAtMin, params.CreatedAtMax)
	}
	if params.CreatedAtMin.After(params.CreatedAtMax) {
		return fmt.Errorf("created_at min after max: min %v, max %v", params.CreatedAtMin, params.CreatedAtMax)
	}
	return nil
}

type ProfileList interface {
	Next() bool
	Profile() (io.Reader, error)
	Close() error
}
