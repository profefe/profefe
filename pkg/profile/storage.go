package profile

import (
	"context"
	"io"
	"time"
)

type QueryRequest struct {
	Service      string
	Type         ProfileType
	Digest       Digest
	Labels       Labels
	CreatedAtMin time.Time
	CreatedAtMax time.Time
	Limit        uint
}

type Queryer interface {
	Query(ctx context.Context, query *QueryRequest) ([]*Profile, error)
}

type Storage interface {
	Queryer
	Create(ctx context.Context, prof *Profile, r io.Reader) error
	Open(ctx context.Context, dgst Digest) (io.ReadCloser, error)
	Delete(ctx context.Context, dgst Digest) error
}
