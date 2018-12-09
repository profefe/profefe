package storage

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/profefe/profefe/pkg/profile"
)

var ErrNotFound = errors.New("not found")

type QueryRequest struct {
	Service      string
	Type         profile.ProfileType
	Digest       profile.Digest
	Labels       profile.Labels
	CreatedAtMin time.Time
	CreatedAtMax time.Time
	Limit        uint
}

type Queryer interface {
	Get(ctx context.Context, dgst profile.Digest) (*profile.Profile, error)
	Query(ctx context.Context, query *QueryRequest) ([]*profile.Profile, error)
}

type Storage interface {
	Queryer
	Create(ctx context.Context, meta map[string]interface{}, r io.Reader) (*profile.Profile, error)
	Open(ctx context.Context, dgst profile.Digest) (io.ReadCloser, error)
	Delete(ctx context.Context, dgst profile.Digest) error
}
