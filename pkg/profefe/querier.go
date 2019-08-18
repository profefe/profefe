package profefe

import (
	"context"
	"io"

	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
	"golang.org/x/xerrors"
)

type Querier struct {
	logger *log.Logger
	sr     storage.Reader
}

func NewQuerier(logger *log.Logger, sr storage.Reader) *Querier {
	return &Querier{
		sr:     sr,
		logger: logger,
	}
}

func (q *Querier) GetServices(ctx context.Context) ([]string, error) {
	return q.sr.GetServices(ctx)
}

func (q *Querier) GetProfile(ctx context.Context, pid profile.ProfileID) (*profile.ProfileFactory, error) {
	return q.sr.GetProfile(ctx, pid)
}

func (q *Querier) FindProfileTo(ctx context.Context, dst io.Writer, params *storage.FindProfilesParams) error {
	ppf, err := q.sr.FindProfiles(ctx, params)
	if err != nil {
		return err
	}

	pps := make([]*pprofProfile.Profile, len(ppf))
	for i, pf := range ppf {
		pps[i], err = pf.Profile()
		if err != nil {
			return err
		}
	}

	pp, err := pprofProfile.Merge(pps)
	if err != nil {
		return xerrors.Errorf("could not merge %d profiles: %w", len(pps), err)
	}
	return pp.Write(dst)
}
