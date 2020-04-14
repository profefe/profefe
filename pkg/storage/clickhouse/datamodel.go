package clickhouse

import (
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/profefe/profefe/pkg/profile"
	"github.com/rs/xid"
)

type ProfileKey [12]byte

func NewProfileKey(t time.Time) (pk ProfileKey) {
	b := xid.NewWithTime(t).Bytes()
	if len(b) != len(pk) {
		panic(fmt.Sprintf("bad profile key length %d", len(b)))
	}
	copy(pk[:], b)
	return pk
}

func (id ProfileKey) Value() (driver.Value, error) {
	return id[:], nil
}

type ProfileType uint8

// Profile types supported by ClickHouse writer.
// Must match with values defined in `pprof_profiles.profile_type` SQL enum.
const (
	TypeCPU          ProfileType = 1
	TypeHeap         ProfileType = 2
	TypeBlock        ProfileType = 3
	TypeMutex        ProfileType = 4
	TypeGoroutine    ProfileType = 5
	TypeThreadcreate ProfileType = 6

	TypeOther ProfileType = 100
)

func ToProfileType(ptype profile.ProfileType) (ProfileType, error) {
	switch ptype {
	case profile.TypeCPU:
		return TypeCPU, nil
	case profile.TypeHeap:
		return TypeHeap, nil
	case profile.TypeBlock:
		return TypeBlock, nil
	case profile.TypeMutex:
		return TypeMutex, nil
	case profile.TypeGoroutine:
		return TypeGoroutine, nil
	case profile.TypeThreadcreate:
		return TypeThreadcreate, nil
	case profile.TypeOther:
		return TypeOther, nil
	default:
		return 0, fmt.Errorf("unsupported profile type %q", ptype)
	}
}
