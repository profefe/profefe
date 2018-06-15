package profile

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ProfileType int

const (
	UnknownProfile = -1

	CPUProfile ProfileType = iota
	HeapProfile
	BlockProfile
	MutexProfile
)

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

type Digest string

type Profile struct {
	Service    string // adjust_server, callback_worker, etc
	BuildID    string // sha1 of the binary
	Generation string // generation within binary's build id
	Type       ProfileType
	CreatedAt  time.Time
	ReceivedAt time.Time

	Labels Labels // arbitrary set of key-value pairs

	Digest Digest // as for now, sha1 of data stored in blob storage
	Size   int64  // size of data stored in blob storage
}

func New() *Profile {
	return NewWithMeta(nil)
}

func NewWithMeta(meta map[string]interface{}) *Profile {
	p := &Profile{
		Type:       UnknownProfile,
		ReceivedAt: time.Now().UTC(),
	}
	parseProfileMeta(p, meta)
	return p
}

func parseProfileMeta(p *Profile, meta map[string]interface{}) {
	if p.Labels == nil {
		p.Labels = make(Labels, 0, len(meta))
	}

	for k, rawVal := range meta {
		val, _ := rawVal.(string)
		switch k {
		case LabelService:
			p.Service = val
		case LabelID:
			p.BuildID = val
		case LabelGeneration:
			p.Generation = val
		case LabelType:
			p.Type.UnmarshalString(val)
		case LabelTime:
			tm, _ := time.Parse(time.RFC3339, val)
			if !tm.IsZero() {
				p.CreatedAt = tm
			}
		default:
			p.Labels = append(p.Labels, Label{k, val})
		}
	}

	if len(p.Labels) > 0 {
		sort.Sort(p.Labels)
	}
}
