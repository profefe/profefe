package profile

import (
	"time"
)

const TestID = ID("bpc00mript33iv4net00")

type ID string

type Meta struct {
	ProfileID ID          `json:"profile_id"`
	Service   string      `json:"service"`
	Type      ProfileType `json:"type"`
	Labels    Labels      `json:"labels,omitempty"`
	CreatedAt time.Time   `json:"created_at,omitempty"`
}
