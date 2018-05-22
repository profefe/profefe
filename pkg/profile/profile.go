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

type Label struct {
	Key, Value string
}

type Profile struct {
	Service      string // adjust_server, callback_worker, etc
	BuildID      string // sha1 of the binary
	BuildVersion int64  // binary execution version

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
			ver, _ := strconv.ParseInt(v, 10, 64)
			if ver > 0 {
				p.BuildVersion = ver
			}
		case "type":
			p.Type = parseProfileType(v)
		case "time":
			sec, _ := strconv.ParseInt(v, 10, 64)
			if sec > 0 {
				p.CreatedAt = time.Unix(0, sec) // FIXME(narqo): this gives a local time, not UTC!
			}
		default:
			p.Labels = append(p.Labels, Label{k, v})
		}
	}
}

func parseProfileType(rawtype string) ProfileType {
	pt, _ := strconv.Atoi(rawtype)
	switch ptype := ProfileType(pt); ptype {
	case CPUProfile, HeapProfile, BlockProfile, MutexProfile:
		return ptype
	}
	return UnknownProfile
}
