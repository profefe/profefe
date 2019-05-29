package profile

import (
	"context"
	"io"
	"time"

	"github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/logger"
	"golang.org/x/xerrors"
)

type Repository struct {
	logger  *logger.Logger
	storage Storage
}

func NewRepository(log *logger.Logger, st Storage) *Repository {
	return &Repository{
		logger:  log,
		storage: st,
	}
}

type CreateProfileRequest struct {
	Service    string
	InstanceID InstanceID
	Type       ProfileType
	Labels     Labels
}

func (req *CreateProfileRequest) Validate() error {
	if req == nil {
		return xerrors.New("nil request")
	}

	if req.Service == "" {
		return xerrors.Errorf("service empty: req %v", req)
	}
	if req.InstanceID.IsNil() {
		return xerrors.Errorf("instance_id empty: req: %v", req)
	}
	if req.Type == UnknownProfile {
		return xerrors.Errorf("unknown profile type %s: req %v", req.Type, req)
	}
	return nil
}

func (repo *Repository) CreateProfile(ctx context.Context, req *CreateProfileRequest, r io.Reader) error {
	pp, err := profile.Parse(r)
	if err != nil {
		return xerrors.Errorf("could not parse profile: %w", err)
	}

	meta := NewProfileMeta(req.Service, req.InstanceID, req.Labels)
	return repo.storage.CreateProfile(ctx, req.Type, meta, pp)
}

type GetProfileRequest struct {
	Service string
	Type    ProfileType
	From    time.Time
	To      time.Time
	Labels  Labels
	Limit   int
}

func (req *GetProfileRequest) Validate() error {
	if req == nil {
		return xerrors.New("nil request")
	}

	if req.Service == "" {
		return xerrors.Errorf("service empty: req %v", req)
	}
	if req.Type == UnknownProfile {
		return xerrors.Errorf("unknown profile type %s: req %v", req.Type, req)
	}
	if req.From.IsZero() || req.To.IsZero() {
		return xerrors.Errorf("createdAt time zero: req %v", req)
	}
	if req.To.Before(req.From) {
		return xerrors.Errorf("createdAt time min after max: req %v", req)
	}
	return nil
}

func (repo *Repository) GetProfile(ctx context.Context, req *GetProfileRequest) (*profile.Profile, error) {
	filter := &GetProfileFilter{
		Service:      req.Service,
		Type:         req.Type,
		Labels:       req.Labels,
		CreatedAtMin: req.From,
		CreatedAtMax: req.To,
		Limit:        uint(req.Limit),
	}
	return repo.storage.GetProfile(ctx, filter)
}

func (repo *Repository) GetProfileTo(ctx context.Context, req *GetProfileRequest, w io.Writer) error {
	pp, err := repo.GetProfile(ctx, req)
	if err != nil {
		return err
	}
	return pp.Write(w)
}
