package profefe

import (
	"bytes"
	"context"
	"io"
	"time"

	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
	"golang.org/x/xerrors"
)

type Collector struct {
	logger *log.Logger
	sw     storage.Writer
}

func NewCollector(logger *log.Logger, sw storage.Writer) *Collector {
	return &Collector{
		logger: logger,
		sw:     sw,
	}
}

func (c *Collector) WriteProfile(ctx context.Context, params *storage.WriteProfileParams, r io.Reader) (Profile, error) {
	if params.Type != profile.TypeTrace && params.CreatedAt.IsZero() {
		var buf bytes.Buffer
		r = io.TeeReader(r, &buf)
		pp, err := pprofProfile.Parse(r)
		if err != nil {
			return Profile{}, xerrors.Errorf("could not parse pprof profile: %w", err)
		}
		if pp.TimeNanos > 0 {
			params.CreatedAt = time.Unix(0, pp.TimeNanos).UTC()
		}
	}

	if params.CreatedAt.IsZero() {
		params.CreatedAt = time.Now().UTC()
	}

	meta, err := c.sw.WriteProfile(ctx, params, r)
	if err != nil {
		return Profile{}, err
	}
	return ProfileFromProfileMeta(meta), nil
}
