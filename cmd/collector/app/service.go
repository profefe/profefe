package app

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/store"
)

type ProfileService struct {
	store *store.Store
}

func NewProfileService(s *store.Store) *ProfileService {
	return &ProfileService{
		store: s,
	}
}

func (svc *ProfileService) ListProfiles(ctx context.Context) ([]*profile.Profile, error) {
	return nil, fmt.Errorf("not implemented")
}

type createProfileRequest struct {
	Meta map[string]interface{} `json:"meta"`
	Data []byte                 `json:"data"`
}

func (svc *ProfileService) CreateProfile(ctx context.Context, req *createProfileRequest) error {
	p, err := svc.store.Create(ctx, req.Meta, req.Data)
	if err != nil {
		return err
	}

	log.Printf("DEBUG create profile: %+v\n", p)

	return nil
}

type getProfileRequest struct {
	Service string
	Type    profile.ProfileType
	From    time.Time
	To      time.Time
	Labels  profile.Labels
}

func (svc *ProfileService) GetProfile(ctx context.Context, req *getProfileRequest) (*profile.Profile, io.ReadCloser, error) {
	query := &profile.QueryRequest{
		Service:      req.Service,
		Type:         req.Type,
		CreatedAtMin: req.From,
		CreatedAtMax: req.To,
	}
	p, pr, err := svc.store.Lookup(ctx, query)
	if err != nil {
		return nil, nil, err
	}

	log.Printf("DEBUG get profile: %+v\n", p)

	return p, pr, nil
}
