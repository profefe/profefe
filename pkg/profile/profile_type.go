package profile

import (
	"fmt"
	"strconv"
	"strings"
)

type ProfileType int

const (
	UnknownProfile ProfileType = iota
	CPUProfile
	HeapProfile
	BlockProfile
	MutexProfile
)

func ProfileTypeFromString(s string) ProfileType {
	s = strings.TrimSpace(s)
	switch s {
	case "cpu":
		return CPUProfile
	case "heap":
		return HeapProfile
	case "block":
		return BlockProfile
	case "mutex":
		return MutexProfile
	default:
		return UnknownProfile
	}
}

func (ptype ProfileType) MarshalString() (s string) {
	return strconv.FormatInt(int64(ptype), 10)
}

func (ptype *ProfileType) UnmarshalString(s string) error {
	pt, err := strconv.Atoi(s)
	if err != nil {
		return err
	}
	switch pt := ProfileType(pt); pt {
	case CPUProfile, HeapProfile, BlockProfile, MutexProfile:
		*ptype = pt
	default:
		*ptype = UnknownProfile
	}
	return nil
}

func (ptype ProfileType) String() string {
	switch ptype {
	case UnknownProfile:
		return "unknown"
	case CPUProfile:
		return "cpu"
	case HeapProfile:
		return "heap"
	case BlockProfile:
		return "block"
	case MutexProfile:
		return "mutex"
	}
	return fmt.Sprintf("%d", ptype)
}
