package storage

import (
	"context"
	"io"
	"time"

	"github.com/profefe/profefe/pkg/profile"
	"golang.org/x/xerrors"
)

var (
	ErrNotFound = xerrors.New("profile not found")
	ErrEmpty    = xerrors.New("profile is empty")
)

type Writer interface {
	WriteProfile(ctx context.Context, ptype profile.ProfileType, meta *profile.ProfileMeta, pf *profile.ProfileFactory) error
}

type Reader interface {
	GetProfile(ctx context.Context, pid profile.ProfileID) (*profile.ProfileFactory, error)
	FindProfile(ctx context.Context, req *FindProfileRequest) (*profile.ProfileFactory, error)
}

type Storage interface {
	Writer
	Reader
}

type FindProfileRequest struct {
	Service      string
	Type         profile.ProfileType
	Labels       profile.Labels
	CreatedAtMin time.Time
	CreatedAtMax time.Time
	Limit        int
}

func (filter *FindProfileRequest) Validate() error {
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

type WriteProfileRequest struct {
	Service    string
	InstanceID profile.InstanceID
	Type       profile.ProfileType
	Labels     profile.Labels
}

func (req *WriteProfileRequest) Validate() error {
	if req == nil {
		return xerrors.New("nil request")
	}

	if req.Service == "" {
		return xerrors.Errorf("service empty: req %v", req)
	}
	if req.InstanceID.IsNil() {
		return xerrors.Errorf("instance_id empty: req: %v", req)
	}
	if req.Type == profile.UnknownProfile {
		return xerrors.Errorf("unknown profile type %s: req %v", req.Type, req)
	}
	return nil
}

func WriteProfileFrom(ctx context.Context, src io.Reader, pw Writer, req *WriteProfileRequest) error {
	pf := profile.NewProfileFactoryFrom(src)
	meta := profile.NewProfileMeta(req.Service, req.InstanceID, req.Labels)
	return pw.WriteProfile(ctx, req.Type, meta, pf)
}

func FindProfileTo(ctx context.Context, dst io.Writer, pr Reader, req *FindProfileRequest) error {
	pf, err := pr.FindProfile(ctx, req)
	if err != nil {
		return err
	}
	return pf.WriteTo(dst)
}
