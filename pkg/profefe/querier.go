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

	if !list.Next() {
		return nil, storage.ErrNotFound
	}
	return list.Profile()
}

func (q *Querier) FindProfiles(ctx context.Context, params *storage.FindProfilesParams) ([]*profile.Meta, error) {
	return q.sr.FindProfiles(ctx, params)
}

func (q *Querier) FindMergeProfileTo(ctx context.Context, dst io.Writer, params *storage.FindProfilesParams) error {
	pids, err := q.sr.FindProfileIDs(ctx, params)
	if err != nil {
		return err
	}

	// TODO(narqo): limit maximum number of profiles to merge; as an example,
	//  Stackdriver merges up to 250 random profiles if query returns more than that
	list, err := q.sr.ListProfiles(ctx, pids)
	if err != nil {
		return err
	}
	defer list.Close()

	pps := make([]*pprofProfile.Profile, 0, len(pids))
	for list.Next() {
		p, err := list.Profile()
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
