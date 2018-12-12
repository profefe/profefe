package profile

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/pprof/profile"
	"github.com/profefe/profefe/pkg/logger"
)

var (
	ErrNotFound = errors.New("profile not found")
	ErrEmpty    = errors.New("profile is empty")
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

func (repo *Repository) ListProfiles(ctx context.Context) ([]*Profile, error) {
	return nil, fmt.Errorf("not implemented")
}

type CreateProfileRequest struct {
	Meta map[string]interface{} `json:"meta"`
	Data []byte                 `json:"data"`
}

func (repo *Repository) CreateProfile(ctx context.Context, req *CreateProfileRequest) (*Profile, error) {
	if len(req.Data) == 0 {
		return nil, ErrEmpty
	}

	pprof, err := profile.ParseData(req.Data)
	if err != nil {
		return nil, fmt.Errorf("could not parse profile: %v", err)
	}

	prof := NewWithMeta(pprof, req.Meta)

	err = repo.storage.Create(ctx, prof, bytes.NewReader(req.Data))
	if err != nil {
		return nil, err
	}

	repo.logger.Debugw("create", "profile", prof)

	return prof, nil
}

type GetProfileRequest struct {
	Service string
	Type    ProfileType
	From    time.Time
	To      time.Time
	Labels  Labels
}

func (req *GetProfileRequest) Validate() error {
	if req == nil {
		return errors.New("nil query request")
	}

	if req.Service == "" {
		return fmt.Errorf("no service: query %v", req)
	}
	if req.Type == UnknownProfile {
		return fmt.Errorf("unknown profile type %s: query %v", req.Type, req)
	}
	if req.From.IsZero() || req.To.IsZero() {
		return fmt.Errorf("createdAt time zero: query %v", req)
	}
	if req.To.Before(req.From) {
		return fmt.Errorf("createdAt time min after max: query %v", req)
	}
	return nil
}

func (repo *Repository) GetProfile(ctx context.Context, req *GetProfileRequest) (*Profile, error) {
	query := &QueryRequest{
		Service:      req.Service,
		Type:         req.Type,
		Labels:       req.Labels,
		CreatedAtMin: req.From,
		CreatedAtMax: req.To,
	}
	ps, err := repo.storage.Query(ctx, query)
	if err != nil {
		return nil, err
	} else if len(ps) == 0 {
		return nil, ErrNotFound
	}

	pprofs := make([]*profile.Profile, 0, len(ps))
	for _, p := range ps {
		rc, err := repo.storage.Open(ctx, p.Digest)
		if err != nil {
			return nil, fmt.Errorf("could not open profile %s: %v", p.Digest, err)
		}
		pprof, err := profile.Parse(rc)
		rc.Close()
		if err != nil {
			return nil, fmt.Errorf("could not parse profile %s: %v", p.Digest, err)
		}
		pprofs = append(pprofs, pprof)
	}

	pprof, err := profile.Merge(pprofs)
	if err != nil {
		return nil, fmt.Errorf("could not merge %d profiles: %v", len(pprofs), err)
	}

	prof := New(pprof)
	// copy only fields that make sense for a merged profile
	prof.Service = ps[0].Service
	prof.Type = ps[0].Type

	repo.logger.Debugw("get", "profile", prof)

	return prof, nil
}
