package profile

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/pprof/profile"
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

type CreateProfileRequest struct {
	ID      string
	Service string
	Labels  Labels
}

func (req *CreateProfileRequest) Validate() error {
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

func (repo *Repository) CreateProfile(ctx context.Context, req *CreateProfileRequest) (token string, err error) {
	service := NewService(req.Service, req.ID, req.Labels)
	prof := &Profile{
		CreatedAt:  time.Now().UTC(),
		ReceivedAt: time.Now().UTC(),
		Service:    service,
	}

	if err := repo.storage.Create(ctx, prof); err != nil {
		return "", err
	}

	repo.logger.Debugw("create profile", "profile", prof)

	return service.Token.String(), nil
}

type UpdateProfileRequest struct {
	ID    string
	Token string
	Type  ProfileType
}

func (req *UpdateProfileRequest) Validate() error {
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

func (repo *Repository) UpdateProfile(ctx context.Context, req *UpdateProfileRequest, r io.Reader) error {
	// TODO(narqo) cap the profile bytes with some sane defaults
	// r = io.LimitReader(r, ??)

	var buf bytes.Buffer
	pprof, err := profile.Parse(io.TeeReader(r, &buf))
	if err != nil {
		return fmt.Errorf("could not parse profile: %v", err)
	}

	prof := &Profile{
		Type:       req.Type,
		ReceivedAt: time.Now().UTC(),
		Service: &Service{
			BuildID: req.ID,
			Token:   TokenFromString(req.Token),
		},
	}

	if pprof.TimeNanos > 0 {
		prof.CreatedAt = time.Unix(0, pprof.TimeNanos).UTC()
	}

	if err := repo.storage.Update(ctx, prof, &buf); err != nil {
		return err
	}

	repo.logger.Debugw("update profile", "profile", prof)

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

func (repo *Repository) GetProfile(ctx context.Context, req *GetProfileRequest) (*Profile, io.Reader, error) {
	query := &QueryRequest{
		Service:      req.Service,
		Type:         req.Type,
		Labels:       req.Labels,
		CreatedAtMin: req.From,
		CreatedAtMax: req.To,
	}
	ps, err := repo.storage.Query(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	if len(ps) == 0 {
		return nil, nil, ErrNotFound
	}

	pprofs := make([]*profile.Profile, 0, len(ps))
	for _, p := range ps {
		rc, err := repo.storage.Open(ctx, p.Digest)
		if err != nil {
			return nil, nil, fmt.Errorf("could not open profile %s: %v", p.Digest, err)
		}
		pprof, err := profile.Parse(rc)
		rc.Close()
		if err != nil {
			return nil, nil, fmt.Errorf("could not parse profile %s: %v", p.Digest, err)
		}
		pprofs = append(pprofs, pprof)
	}

	pprof, err := profile.Merge(pprofs)
	if err != nil {
		return nil, nil, fmt.Errorf("could not merge %d profiles: %v", len(pprofs), err)
	}

	// copy only fields that make sense for a merged profile
	prof := &Profile{
		Type: ps[0].Type,
		Service: &Service{
			Name: req.Service,
		},
	}

	return prof, &pprofReader{prof: pprof}, nil
}

type pprofReader struct {
	buf  bytes.Buffer
	prof *profile.Profile
}

func (r *pprofReader) Read(p []byte) (n int, err error) {
	if err := r.prof.Write(&r.buf); err != nil {
		return 0, err
	}
	return r.buf.Read(p)
}

func (r *pprofReader) WriteTo(w io.Writer) (n int64, err error) {
	err = r.prof.Write(w)
	return 0, err
}
