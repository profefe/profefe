package profile

import (
	"io"
	"time"

	"github.com/profefe/profefe/internal/pprof/profile"
	"github.com/rs/xid"
	"golang.org/x/xerrors"
)

type ProfileID []byte

func NewProfileID() ProfileID {
	return ProfileID(xid.New().Bytes())
}

func (pid *ProfileID) FromString(s string) error {
	id, err := xid.FromString(s)
	if err != nil {
		return err
	}
	*pid = id.Bytes()
	return nil
}

func (pid ProfileID) String() string {
	id, _ := xid.FromBytes([]byte(pid))
	return id.String()
}

type InstanceID string

func NewInstanceID() InstanceID {
	return InstanceID(xid.New().String())
}

func (iid InstanceID) IsNil() bool {
	return iid == ""
}

func (iid InstanceID) String() string {
	return string(iid)
}

type ProfileMeta struct {
	ProfileID  ProfileID   `json:"profile_id"`
	Service    string      `json:"service"`
	Type       ProfileType `json:"type"`
	InstanceID InstanceID  `json:"instance_id"`
	Labels     Labels      `json:"labels,omitempty"`
	CreatedAt  time.Time   `json:"created_at,omitempty"`
}

func NewProfileMeta(service string, ptyp ProfileType, iid InstanceID, labels Labels) *ProfileMeta {
	return &ProfileMeta{
		ProfileID:  NewProfileID(),
		Service:    service,
		Type:       ptyp,
		InstanceID: iid,
		Labels:     labels,
		CreatedAt:  time.Now().UTC(),
	}
}

type ProfileFactory struct {
	pp *profile.Profile
	r  io.Reader
}

func NewProfileFactory(pp *profile.Profile) *ProfileFactory {
	return &ProfileFactory{pp: pp}
}

func NewProfileFactoryFrom(r io.Reader) *ProfileFactory {
	return &ProfileFactory{r: r}
}

func (p *ProfileFactory) Profile() (*profile.Profile, error) {
	if p.pp != nil {
		return p.pp, nil
	}
	err := p.parse(p.r)
	return p.pp, err
}

func (p *ProfileFactory) WriteTo(dst io.Writer) error {
	if p.pp != nil {
		return p.pp.Write(dst)
	}
	return p.parse(io.TeeReader(p.r, dst))
}

func (p *ProfileFactory) parse(r io.Reader) (err error) {
	if p.pp != nil {
		return nil
	}
	// TODO(narqo): check if profile.Profile.Compact make any sense here
	p.pp, err = profile.Parse(r)
	if err != nil {
		err = xerrors.Errorf("could not parse profile: %w", err)
	}
	return err
}
