package profile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/profefe/profefe/pkg/logger"
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

type CreateServiceRequest struct {
	ID      string
	Service string
	Labels  Labels
}

func (req *CreateServiceRequest) Validate() error {
	if req == nil {
		return errors.New("nil request")
	}

	if req.ID == "" {
		return fmt.Errorf("id empty: req %v", req)
	}
	if req.Service == "" {
		return fmt.Errorf("service empty: req %v", req)
	}
	return nil
}

func (repo *Repository) CreateService(ctx context.Context, req *CreateServiceRequest) (token string, err error) {
	service := NewService(req.Service, req.ID, req.Labels)

	if err := repo.storage.CreateService(ctx, service); err != nil {
		return "", err
	}
	return service.Token.String(), nil
}

type CreateProfileRequest struct {
	ID    string
	Token string
	Type  ProfileType
}

func (req *CreateProfileRequest) Validate() error {
	if req == nil {
		return errors.New("nil request")
	}

	if req.ID == "" {
		return fmt.Errorf("id empty: req: %v", req)
	}
	if req.Token == "" {
		return fmt.Errorf("token empty: req: %v", req)
	}
	if req.Type == UnknownProfile {
		return fmt.Errorf("unknown profile type %s: req %v", req.Type, req)
	}
	return nil
}

func (repo *Repository) CreateProfile(ctx context.Context, req *CreateProfileRequest, r io.Reader) error {
	prof := &Profile{
		Type: req.Type,
		Service: &Service{
			BuildID: req.ID,
			Token:   TokenFromString(req.Token),
		},
	}

	// TODO(narqo) cap the profile bytes with some sane defaults
	// r = io.LimitReader(r, ??)
	return repo.storage.CreateProfile(ctx, prof, r)
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
		return errors.New("nil request")
	}

	if req.Service == "" {
		return fmt.Errorf("no service: req %v", req)
	}
	if req.Type == UnknownProfile {
		return fmt.Errorf("unknown profile type %s: req %v", req.Type, req)
	}
	if req.From.IsZero() || req.To.IsZero() {
		return fmt.Errorf("createdAt time zero: req %v", req)
	}
	if req.To.Before(req.From) {
		return fmt.Errorf("createdAt time min after max: req %v", req)
	}
	return nil
}

func (repo *Repository) GetProfile(ctx context.Context, req *GetProfileRequest) (io.Reader, error) {
	filter := &ReadProfileFilter{
		Service:      req.Service,
		Type:         req.Type,
		Labels:       req.Labels,
		CreatedAtMin: req.From,
		CreatedAtMax: req.To,
	}
	return repo.storage.ReadProfile(ctx, filter)
}
