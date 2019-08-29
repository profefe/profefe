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

func (q *Querier) GetProfile(ctx context.Context, pid profile.ID) (*pprofProfile.Profile, error) {
	list, err := q.sr.ListProfiles(ctx, []profile.ID{pid})
	if err != nil {
		return nil, err
	}
	defer list.Close()

	return list.Next()
}

func (q *Querier) FindProfiles(ctx context.Context, params *storage.FindProfilesParams) ([]*profile.Meta, error) {
	return q.sr.FindProfiles(ctx, params)
}

func (q *Querier) FindProfileTo(ctx context.Context, dst io.Writer, params *storage.FindProfilesParams) error {
	metas, err := q.sr.FindProfiles(ctx, params)
	if err != nil {
		return err
	}

	pids := make([]profile.ID, len(metas))
	for i, meta := range metas {
		pids[i] = meta.ProfileID
	}
	list, err := q.sr.ListProfiles(ctx, pids)
	if err != nil {
		return err
	}
	defer list.Close()

	pps := make([]*pprofProfile.Profile, 0, len(pids))
	for {
		p, err := list.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		pps = append(pps, p)
	}

	pp, err := pprofProfile.Merge(pps)
	if err != nil {
		return xerrors.Errorf("could not merge %d profiles: %w", len(pps), err)
	}
	return pp.Write(dst)
}
