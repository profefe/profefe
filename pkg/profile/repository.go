package profile

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/pprof/profile"
)

var (
	ErrNotFound = errors.New("profile not found")
	ErrEmpty    = errors.New("profile is empty")
)

type Repository struct {
	storage Storage
}

func NewRepository(s Storage) *Repository {
	return &Repository{
		storage: s,
	}
}

func (repo *Repository) ListProfiles(ctx context.Context) ([]*Profile, error) {
	return nil, fmt.Errorf("not implemented")
}

type CreateProfileRequest struct {
	Meta map[string]interface{} `json:"meta"`
	Data []byte                 `json:"data"`
}

func (repo *Repository) CreateProfile(ctx context.Context, req *CreateProfileRequest) error {
	if len(req.Data) == 0 {
		return ErrEmpty
	}

	prof, err := profile.ParseData(req.Data)
	if err != nil {
		return fmt.Errorf("could not parse profile: %v", err)
	}

	p := NewWithMeta(prof, req.Meta)

	err = repo.storage.Create(ctx, p, bytes.NewReader(req.Data))
	if err != nil {
		return err
	}

	log.Printf("DEBUG create profile: %+v\n", p)

	return nil
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

	profSrcs := make([]*profile.Profile, 0, len(ps))
	for _, p := range ps {
		pr, err := repo.storage.Open(ctx, p.Digest)
		if err != nil {
			return nil, fmt.Errorf("could not open profile %s: %v", p.Digest, err)
		}
		prof, err := profile.Parse(pr)
		pr.Close()
		if err != nil {
			return nil, fmt.Errorf("could not parse profile %s: %v", p.Digest, err)
		}
		profSrcs = append(profSrcs, prof)
	}

	prof, err := profile.Merge(profSrcs)
	if err != nil {
		return nil, fmt.Errorf("could not merge %d profiles: %v", len(profSrcs), err)
	}

	p := New(prof)
	// copy only fields that make sense for a merged profile
	p.Service = ps[0].Service
	p.Type = ps[0].Type

	log.Printf("DEBUG get profile: %+v\n", p)

	return p, nil
}
