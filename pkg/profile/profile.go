package profile

import (
	"time"

	"github.com/rs/xid"
)

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
	Service    string     `json:"service"`
	InstanceID InstanceID `json:"instance_id"`
	Labels     Labels     `json:"labels,omitempty"`
	CreatedAt  time.Time  `json:"created_at,omitempty"`
}

func NewProfileMeta(service string, iid InstanceID, labels Labels) *ProfileMeta {
	return &ProfileMeta{
		Service:    service,
		InstanceID: iid,
		Labels:     labels,
		CreatedAt:  time.Now().UTC(),
	}
}
