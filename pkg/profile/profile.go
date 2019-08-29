package profile

import (
	"time"

	"github.com/rs/xid"
)

type ID []byte

func NewProfileID() ID {
	return xid.New().Bytes()
}

func (pid *ID) FromBytes(b []byte) error {
	id, err := xid.FromBytes(b)
	if err != nil {
		return err
	}
	*pid = id.Bytes()
	return nil
}

func (pid *ID) FromString(s string) error {
	id, err := xid.FromString(s)
	if err != nil {
		return err
	}
	*pid = id.Bytes()
	return nil
}

func (pid ID) IsNil() bool {
	return pid == nil
}

func (pid ID) MarshalJSON() (b []byte, err error) {
	id, err := xid.FromBytes(pid)
	if err != nil {
		return nil, err
	}
	return id.MarshalJSON()
}

func (pid ID) String() string {
	id, _ := xid.FromBytes(pid)
	return id.String()
}

type InstanceID []byte

func NewInstanceID() InstanceID {
	return xid.New().Bytes()
}

func (iid InstanceID) IsNil() bool {
	return iid == nil
}

func (iid InstanceID) String() string {
	return string(iid)
}

type Meta struct {
	ProfileID  ID          `json:"profile_id"`
	Service    string      `json:"service"`
	Type       ProfileType `json:"type"`
	InstanceID InstanceID  `json:"instance_id"`
	Labels     Labels      `json:"labels,omitempty"`
	CreatedAt  time.Time   `json:"created_at,omitempty"`
}

func NewProfileMeta(service string, ptyp ProfileType, iid InstanceID, labels Labels) *Meta {
	return &Meta{
		ProfileID:  NewProfileID(),
		Service:    service,
		Type:       ptyp,
		InstanceID: iid,
		Labels:     labels,
		CreatedAt:  time.Now().UTC(),
	}
}
