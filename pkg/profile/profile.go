package profile

import (
	"bytes"
	"encoding/base32"
	"time"

	"github.com/rs/xid"
)

const encoder = "0123456789abcdefghijklmnopqrstuv"

var encoding = base32.NewEncoding(encoder).WithPadding(base32.NoPadding)

type ID []byte

func NewID() ID {
	return xid.New().Bytes()
}

func IDFromString(s string) (ID, error) {
	return encoding.DecodeString(s)
}

func IDFromBytes(b []byte) (pid ID, err error) {
	err = pid.UnmarshalText(b)
	return pid, err
}

func (pid ID) IsNil() bool {
	return pid == nil
}

func (pid ID) MarshalText() ([]byte, error) {
	buf := make([]byte, encoding.EncodedLen(len(pid)))
	encoding.Encode(buf, pid)
	return buf, nil
}

func (pid *ID) UnmarshalText(b []byte) error {
	buf := make([]byte, encoding.DecodedLen(len(b)))
	_, err := encoding.Decode(buf, b)
	*pid = buf
	return err
}

func (pid ID) MarshalJSON() ([]byte, error) {
	if pid.IsNil() {
		return []byte("null"), nil
	}
	buf := make([]byte, encoding.EncodedLen(len(pid))+2)
	buf[0] = '"'
	encoding.Encode(buf[1:len(buf)-1], pid)
	buf[len(buf)-1] = '"'
	return buf, nil
}

func (pid *ID) UnmarshalJSON(b []byte) error {
	if bytes.Equal(b, []byte("null")) {
		return nil
	}
	return pid.UnmarshalText(b[1 : len(b)-1])
}

func (pid ID) String() string {
	text, _ := pid.MarshalText()
	return string(text)
}

type InstanceID []byte

func NewInstanceID() InstanceID {
	return xid.New().Bytes()
}

func (iid InstanceID) IsNil() bool {
	return iid == nil
}

func (iid InstanceID) String() string {
	return encoding.EncodeToString(iid)
}

type Meta struct {
	ProfileID  ID          `json:"profile_id"`
	Service    string      `json:"service"`
	Type       ProfileType `json:"type"`
	InstanceID InstanceID  `json:"instance_id"`
	Labels     Labels      `json:"labels,omitempty"`
	CreatedAt  time.Time   `json:"created_at,omitempty"`
}

func NewProfileMeta(service string, ptyp ProfileType, iid InstanceID, labels Labels) Meta {
	return Meta{
		ProfileID:  NewID(),
		Service:    service,
		Type:       ptyp,
		InstanceID: iid,
		Labels:     labels,
		CreatedAt:  time.Now().UTC(),
	}
}
