package store

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/profefe/profefe/pkg/profile"
)

var ErrNotFound = errors.New("not found")

type Creater interface {
	Create(ctx context.Context, meta map[string]interface{}, data []byte) (*profile.Profile, error)
}

type Opener interface {
	Open(ctx context.Context, dgst profile.Digest) (io.ReadCloser, error)
}

type Queryer interface {
	Get(ctx context.Context, dgst profile.Digest) (*profile.Profile, error)
	List(ctx context.Context, filter func(*profile.Profile) bool) ([]*profile.Profile, error)
	Query(ctx context.Context, query *QueryRequest) ([]*profile.Profile, error)
}

type QueryRequest struct {
	Service      string
	Type         profile.ProfileType
	Digest       profile.Digest
	CreatedAtMin time.Time
	CreatedAtMax time.Time
	Limit        uint
}

type Deleter interface {
	Delete(ctx context.Context, dgst profile.Digest) error
}

type Store interface {
	Creater
	Opener
	Queryer
	Deleter
}
