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
		logger: logger,
		sr:     sr,
	}
}

func (q *Querier) GetProfilesTo(ctx context.Context, dst io.Writer, pids []profile.ID) error {
	list, err := q.sr.ListProfiles(ctx, pids)
	if err != nil {
		return err
	}
	defer list.Close()

	if len(pids) == 1 {
		if !list.Next() {
			return storage.ErrNotFound
		}
		pr, err := list.Profile()
		if err != nil {
			return err
		}
		_, err = io.Copy(dst, pr)
		return err
	}

	// TODO(narqo): limit maximum number of profiles to merge; as an example,
	//  Stackdriver merges up to 250 random profiles if query returns more than that
	pps := make([]*pprofProfile.Profile, 0, len(pids))
	for list.Next() {
		// exit fast if context canceled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		pr, err := list.Profile()
		if err != nil {
			return err
		}
		p, err := pprofProfile.Parse(pr)
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

func (q *Querier) FindProfiles(ctx context.Context, params *storage.FindProfilesParams) ([]Profile, error) {
	metas, err := q.sr.FindProfiles(ctx, params)
	if err != nil {
		return nil, err
	}

	profModels := make([]Profile, 0, len(metas))
	for _, meta := range metas {
		profModels = append(profModels, ProfileFromProfileMeta(meta))
	}
	return profModels, nil
}

func (q *Querier) FindMergeProfileTo(ctx context.Context, dst io.Writer, params *storage.FindProfilesParams) error {
	pids, err := q.sr.FindProfileIDs(ctx, params)
	if err != nil {
		return err
	}

	return q.GetProfilesTo(ctx, dst, pids)
}

func (q *Querier) ListServices(ctx context.Context) ([]string, error) {
	return q.sr.ListServices(ctx)
}
