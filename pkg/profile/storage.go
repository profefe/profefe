package profile

import (
	"context"
	"errors"
	"io"
	"time"
)

var (
	ErrNotFound = errors.New("profile not found")
	ErrEmpty    = errors.New("profile is empty")
)

type QueryRequest struct {
	Service      string
	Type         ProfileType
	Labels       Labels
	CreatedAtMin time.Time
	CreatedAtMax time.Time
	Limit        uint
}

type Queryer interface {
	Query(ctx context.Context, query *QueryRequest) (io.Reader, error)
}

type Storage interface {
	Queryer
	Create(ctx context.Context, prof *Profile) error
	Update(ctx context.Context, prof *Profile, r io.Reader) error
	Delete(ctx context.Context, prof *Profile) error
}
