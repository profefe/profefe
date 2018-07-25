package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/store"
)

type ProfileService struct {
	store store.Store
}

func NewProfileService(s store.Store) *ProfileService {
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

func validateGetProfileRequest(req *getProfileRequest) error {
	if req == nil {
		return errors.New("nil query request")
	}

	if req.Service == "" {
		return fmt.Errorf("no service: query %v", req)
	}
	if req.Type == profile.UnknownProfile {
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

func (svc *ProfileService) GetProfile(ctx context.Context, req *getProfileRequest) (*profile.Profile, io.ReadCloser, error) {
	if err := validateGetProfileRequest(req); err != nil {
		return nil, nil, err
	}

	query := &store.QueryRequest{
		Service:      req.Service,
		Type:         req.Type,
		CreatedAtMin: req.From,
		CreatedAtMax: req.To,
	}
	ps, err := svc.store.Query(ctx, query)
	if err != nil {
		return nil, nil, err
	}

	if len(ps) == 0 {
		return nil, nil, store.ErrNotFound
	} else if len(ps) > 1 {
		log.Printf("lookup: found %d profiles by query %v", len(ps), query)
	}

	p := ps[0]
	pr, err := svc.store.Open(ctx, p.Digest)
	if err != nil {
		return nil, nil, fmt.Errorf("could not open profile %s: %v", p.Digest, err)
	}

	log.Printf("DEBUG get profile: %+v\n", p)

	return p, pr, nil
}
