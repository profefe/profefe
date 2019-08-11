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
	pr     storage.Reader
}

func NewQuerier(logger *log.Logger, pr storage.Reader) *Querier {
	return &Querier{
		pr:     pr,
		logger: logger,
	}
}

func (q *Querier) GetProfile(ctx context.Context, pid profile.ProfileID) (*profile.ProfileFactory, error) {
	return q.pr.GetProfile(ctx, pid)
}

func (q *Querier) FindProfileTo(ctx context.Context, dst io.Writer, req *storage.FindProfilesParams) error {
	ppf, err := q.pr.FindProfiles(ctx, req)
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
