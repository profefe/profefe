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

func (c *Collector) CollectProfileFrom(ctx context.Context, src io.Reader, req *WriteProfileRequest) error {
	pf := profile.NewProfileFactoryFrom(src)
	return c.sw.WriteProfile(ctx, req.NewProfileMeta(), pf)
}

type WriteProfileRequest struct {
	Service    string
	Type       profile.ProfileType
	InstanceID profile.InstanceID
	Labels     profile.Labels
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

	iid, err := getInstanceID(q)
	if err != nil {
		return err
	}
	req.InstanceID = iid

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
	if req.InstanceID.IsNil() {
		return xerrors.Errorf("instance_id empty: req: %v", req)
	}
	if req.Type == profile.UnknownProfile {
		return xerrors.Errorf("unknown profile type %s: req %v", req.Type, req)
	}
	return nil
}

func (req *WriteProfileRequest) NewProfileMeta() *profile.ProfileMeta {
	return profile.NewProfileMeta(req.Service, req.Type, req.InstanceID, req.Labels)
}
