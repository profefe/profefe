package profile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/profefe/profefe/internal/pprof/profile"
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

type GetServicesRequest struct {
	Service string
}

func (repo *Repository) GetServices(ctx context.Context, req *GetServicesRequest) ([]*Service, error) {
	filter := &GetServicesFilter{
		Service: req.Service,
	}
	services, err := repo.storage.GetServices(ctx, filter)
	if err != nil {
		return nil, err
	}

	return mergeServices(services), nil
}

func mergeServices(services []*Service) []*Service {
	if len(services) == 0 {
		return services
	}

	servicesSet := make(map[string]*Service)
	for _, s1 := range services {
		if servicesSet[s1.Name] == nil {
			servicesSet[s1.Name] = s1
			continue
		}
		s2 := servicesSet[s1.Name]
		s2.Labels = s2.Labels.Add(s1.Labels)
	}

	services = services[:0]

	for _, s := range servicesSet {
		services = append(services, s)
	}

	return services
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

	pp, err := profile.Parse(r)
	if err != nil {
		return fmt.Errorf("could not parse profile: %v", err)
	}

	return repo.storage.CreateProfile(ctx, prof, pp)
}

type GetProfilesRequest struct {
	Service string
	Type    ProfileType
	From    time.Time
	To      time.Time
	Labels  Labels
	Limit   int
}

func (repo *Repository) GetProfiles(ctx context.Context, req *GetProfilesRequest) ([]*profile.Profile, error) {
	panic("not implemented")
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

func (repo *Repository) GetProfile(ctx context.Context, req *GetProfileRequest) (*profile.Profile, error) {
	filter := &GetProfileFilter{
		Service:      req.Service,
		Type:         req.Type,
		Labels:       req.Labels,
		CreatedAtMin: req.From,
		CreatedAtMax: req.To,
	}
	return repo.storage.GetProfile(ctx, filter)
}

func (repo *Repository) GetProfileTo(ctx context.Context, req *GetProfileRequest, w io.Writer) error {
	filter := &GetProfileFilter{
		Service:      req.Service,
		Type:         req.Type,
		Labels:       req.Labels,
		CreatedAtMin: req.From,
		CreatedAtMax: req.To,
	}
	pp, err := repo.storage.GetProfile(ctx, filter)
	if err != nil {
		return err
	}
	return pp.Write(w)
}
