package profile

import (
	"fmt"
	"strconv"
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

func (ptype ProfileType) MarshalString() (s string, err error) {
	return strconv.FormatInt(int64(ptype), 10), nil
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

type Label struct {
	Key, Value string
}

type Profile struct {
	Service      string // adjust_server, callback_worker, etc
	BuildID      string // sha1 of the binary
	BuildVersion string // binary version

	Type       ProfileType
	CreatedAt  time.Time
	ReceivedAt time.Time

	Labels []Label // arbitrary set of key-value pairs

	Digest string // currently, sha1 of data stored in blobstore
	Size   int64  // size of data stored in blobstore
}

func New() *Profile {
	return NewWithMeta(nil)
}

func NewWithMeta(meta map[string]string) *Profile {
	p := &Profile{
		Type:       UnknownProfile,
		ReceivedAt: time.Now().UTC(),
	}
	parseProfileMeta(p, meta)
	return p
}

func parseProfileMeta(p *Profile, meta map[string]string) {
	if p.Labels == nil {
		p.Labels = make([]Label, 0, len(meta))
	}
	for k, v := range meta {
		switch k {
		case "service":
			p.Service = v
		case "build_id":
			p.BuildID = v
		case "build_version":
			p.BuildVersion = v
		case "type":
			p.Type.UnmarshalString(v)
		case "time":
			tm, _ := time.Parse(time.RFC3339, v)
			if !tm.IsZero() {
				p.CreatedAt = tm
			}
		default:
			p.Labels = append(p.Labels, Label{k, v})
		}
	}
}
