package profile

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"time"

	pprof "github.com/google/pprof/profile"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
)

type Service struct {
	storage storage.Storage
}

func NewProfileService(s storage.Storage) *Service {
	return &Service{
		storage: s,
	}
}

func (svc *Service) ListProfiles(ctx context.Context) ([]*profile.Profile, error) {
	return nil, fmt.Errorf("not implemented")
}

type createProfileRequest struct {
	Meta map[string]interface{} `json:"meta"`
	Data []byte                 `json:"data"`
}

func (svc *Service) CreateProfile(ctx context.Context, req *createProfileRequest) error {
	if len(req.Data) == 0 {
		return errors.New("empty data")
	}
	p, err := svc.storage.Create(ctx, req.Meta, bytes.NewReader(req.Data))
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

func (req *getProfileRequest) Validate() error {
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

func (svc *Service) GetProfile(ctx context.Context, req *getProfileRequest) (*profile.Profile, io.ReadCloser, error) {
	query := &storage.QueryRequest{
		Service:      req.Service,
		Type:         req.Type,
		Labels:       req.Labels,
		CreatedAtMin: req.From,
		CreatedAtMax: req.To,
	}
	ps, err := svc.storage.Query(ctx, query)
	if err != nil {
		return nil, nil, err
	}

	if len(ps) == 0 {
		return nil, nil, storage.ErrNotFound
	} else if len(ps) > 1 {
		log.Printf("lookup: found %d profiles by query %v", len(ps), query)
	}

	profs := make([]*pprof.Profile, 0, len(ps))
	for _, p := range ps {
		pr, err := svc.storage.Open(ctx, p.Digest)
		if err != nil {
			return nil, nil, fmt.Errorf("could not open profile %s: %v", p.Digest, err)
		}
		prof, err := pprof.Parse(pr)
		pr.Close()
		if err != nil {
			return nil, nil, fmt.Errorf("could not parse profile %s: %v", p.Digest, err)
		}
		profs = append(profs, prof)
	}

	prof, err := pprof.Merge(profs)
	if err != nil {
		return nil, nil, fmt.Errorf("could not merge %d profiles: %v", len(profs), err)
	}

	p := profile.New(prof)
	p.Type = ps[0].Type
	p.Labels = ps[0].Labels

	log.Printf("DEBUG get profile: %+v\n", p)

	var buf bytes.Buffer
	if err := prof.Write(&buf); err != nil {
		return nil, nil, err
	}

	return p, ioutil.NopCloser(&buf), nil
}
