package profefe

import (
	"context"
	"io"
	"net/url"

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
	Service string
	Type    profile.ProfileType
	Labels  profile.Labels
}

func (req *WriteProfileRequest) UnmarshalURL(q url.Values) error {
	if req == nil {
		return xerrors.New("nil request")
	}

	*req = WriteProfileRequest{
		Service: q.Get("service"),
		Type:    profile.UnknownProfile,
		Labels:  nil,
	}

	ptype, err := getProfileType(q)
	if err != nil {
		return err
	}
	req.Type = ptype

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
	if req.Type == profile.UnknownProfile {
		return xerrors.Errorf("unknown profile type %s: req %v", req.Type, req)
	}
	return nil
}

func (req *WriteProfileRequest) NewProfileMeta() profile.Meta {
	return profile.NewProfileMeta(req.Service, req.Type, req.Labels)
}
