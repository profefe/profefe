package profile

import (
	"fmt"
	"strings"
)

type ProfileType int8

const (
	UnknownProfile ProfileType = iota
	CPUProfile
	HeapProfile
	BlockProfile
	MutexProfile
	GoroutineProfile
	ThreadcreateProfile
	AllocsProfile

	OtherProfile = 127
)

func (ptype *ProfileType) FromString(s string) error {
	s = strings.TrimSpace(s)
	switch s {
	case "allocs":
		*ptype = AllocsProfile
	case "cpu":
		*ptype = CPUProfile
	case "heap":
		*ptype = HeapProfile
	case "block":
		*ptype = BlockProfile
	case "mutex":
		*ptype = MutexProfile
	case "goroutine":
		*ptype = GoroutineProfile
	case "threadcreate":
		*ptype = ThreadcreateProfile
	case "other":
		*ptype = OtherProfile
	default:
		*ptype = UnknownProfile
	}
	return nil
}

func (ptype ProfileType) String() string {
	switch ptype {
	case UnknownProfile:
		return "unknown"
	case AllocsProfile:
		return "allocs"
	case CPUProfile:
		return "cpu"
	case HeapProfile:
		return "heap"
	case BlockProfile:
		return "block"
	case MutexProfile:
		return "mutex"
	case GoroutineProfile:
		return "goroutine"
	case ThreadcreateProfile:
		return "threadcreate"
	case OtherProfile:
		return "other"
	}
	return fmt.Sprintf("%d", ptype)
}
