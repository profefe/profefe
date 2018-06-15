package profile

import (
	"context"
	"io"
	"time"
)

type Creator interface {
	Create(ctx context.Context, meta map[string]interface{}, data []byte) (*Profile, error)
}

type Opener interface {
	Open(ctx context.Context, dgst Digest) (io.ReadCloser, error)
}

type Queryor interface {
	Get(ctx context.Context, dgst Digest) (*Profile, error)
	List(ctx context.Context, filter func(*Profile) bool) ([]*Profile, error)
	Query(ctx context.Context, query *QueryRequest) ([]*Profile, error)
}

type QueryRequest struct {
	Service      string
	Type         ProfileType
	Digest       Digest
	CreatedAtMin time.Time
	CreatedAtMax time.Time
	Limit        uint32
}

type Deletor interface {
	Delete(ctx context.Context, dgst Digest) error
}

type Repo interface {
	Creator
	Opener
	Queryor
	Deletor
}
