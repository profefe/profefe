package profile

import (
	"fmt"
	"strings"
)

type ProfileType uint8

const (
	TypeUnknown ProfileType = iota
	TypeCPU
	TypeHeap
	TypeBlock
	TypeMutex
	TypeGoroutine
	TypeThreadcreate

	TypeOther ProfileType = 127
	TypeTrace ProfileType = 128
)

func (ptype *ProfileType) FromString(s string) error {
	s = strings.TrimSpace(s)
	switch s {
	case "cpu":
		*ptype = TypeCPU
	case "heap":
		*ptype = TypeHeap
	case "block":
		*ptype = TypeBlock
	case "mutex":
		*ptype = TypeMutex
	case "goroutine":
		*ptype = TypeGoroutine
	case "threadcreate":
		*ptype = TypeThreadcreate
	case "other":
		*ptype = TypeOther
	case "trace":
		*ptype = TypeTrace
	default:
		*ptype = TypeUnknown
	}
	return nil
}

func (ptype ProfileType) String() string {
	switch ptype {
	case TypeUnknown:
		return "unknown"
	case TypeCPU:
		return "cpu"
	case TypeHeap:
		return "heap"
	case TypeBlock:
		return "block"
	case TypeMutex:
		return "mutex"
	case TypeGoroutine:
		return "goroutine"
	case TypeThreadcreate:
		return "threadcreate"
	case TypeOther:
		return "other"
	case TypeTrace:
		return "trace"
	}
	return fmt.Sprintf("ProfileType(%d)", ptype)
}
