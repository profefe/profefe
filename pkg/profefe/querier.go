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
	return q.sr.ListServices(ctx)
}

func (q *Querier) GetProfile(ctx context.Context, pid profile.ID) (*pprofProfile.Profile, error) {
	pr, err := q.sr.ListProfiles(ctx, []profile.ID{pid})
	if err != nil {
		return nil, err
	}
	defer pr.Close()

	var pp *pprofProfile.Profile
	for pr.Next() {
		pp = pr.Profile()
	}
	return pp, pr.Err()
}

func (q *Querier) FindProfileTo(ctx context.Context, dst io.Writer, params *storage.FindProfilesParams) error {
	pr, err := q.sr.FindProfiles(ctx, params)
	if err != nil {
		return err
	}
	defer pr.Close()

	pps := make([]*pprofProfile.Profile, 0)
	for pr.Next() {
		pps = append(pps, pr.Profile())
	}
	if err := pr.Err(); err != nil {
		return err
	}

	pp, err := pprofProfile.Merge(pps)
	if err != nil {
		return xerrors.Errorf("could not merge %d profiles: %w", len(pps), err)
	}
	return pp.Write(dst)
}
