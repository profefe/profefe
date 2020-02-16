package profefe

import (
	"context"
	"io"
	"net/url"
	"time"

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

func (c *Collector) CollectProfileFrom(ctx context.Context, src io.Reader, req *WriteProfileRequest) (Profile, error) {
	meta := req.NewProfileMeta()
	if err := c.sw.WriteProfile(ctx, meta, src); err != nil {
		return Profile{}, err
	}
	return ProfileFromProfileMeta(meta), nil
}

type WriteProfileRequest struct {
	Service   string
	Type      profile.ProfileType
	CreatedAt time.Time
	Labels    profile.Labels
}

func (req *WriteProfileRequest) UnmarshalURL(q url.Values) error {
	if req == nil {
		return xerrors.New("nil request")
	}

	*req = WriteProfileRequest{
		Service: q.Get("service"),
		Type:    profile.TypeUnknown,
		Labels:  nil,
	}

	ptype, err := getProfileType(q)
	if err != nil {
		return err
	}
	req.Type = ptype

	if v := q.Get("created_at"); v != "" {
		tm, err := parseTime(v)
		if err != nil {
			return err
		}
		req.CreatedAt = tm
	}

	labels, err := getLabels(q)
	if err != nil {
		return err
	}
	req.Labels = labels

	return req.Validate()
}

func (req *WriteProfileRequest) Validate() error {
	if req == nil {
		return xerrors.New("nil request")
	}

	if req.Service == "" {
		return xerrors.Errorf("service empty: req %v", req)
	}
	if req.Type == profile.TypeUnknown {
		return xerrors.Errorf("unknown profile type %s: req %v", req.Type, req)
	}
	return nil
}

func (req *WriteProfileRequest) NewProfileMeta() profile.Meta {
	meta := profile.NewProfileMeta(req.Service, req.Type, req.Labels)
	if !req.CreatedAt.IsZero() {
		meta.CreatedAt = req.CreatedAt
	}
	return meta
}
