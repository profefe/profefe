package store

import (
	"strconv"
	"time"
)

type ProfileType int

const (
	UnknownProfile = -1

	CPUProfile ProfileType = iota
	MemProfile
)

type Label struct {
	Key, Value string
}

type Profile struct {
	Name    string // adjust_server, callback_worker, etc
	ID      string // sha1 of the binary
	Version int64  // binary execution version

	Type       ProfileType
	CreatedAt  time.Time
	ReceivedAt time.Time

	Labels []Label // arbitrary set of key-value pairs

	Hash string // sha1 of data stored in blobstore
	Size int64  // size of data stored in blobstore
}

func parseProfileMeta(p *Profile, meta map[string]string) {
	if p.Labels == nil {
		p.Labels = make([]Label, 0, len(meta))
	}
	for k, v := range meta {
		switch k {
		case "name":
			p.Name = v
		case "id":
			p.ID = v
		case "version":
			ver, _ := strconv.ParseInt(v, 10, 64)
			p.Version = ver
		case "type":
			p.Type = parseProfileType(v)
		case "time":
			sec, _ := strconv.ParseInt(v, 10, 64)
			p.CreatedAt = time.Unix(0, sec)
		default:
			p.Labels = append(p.Labels, Label{k, v})
		}
	}
}

// TODO: parse profile type
func parseProfileType(ptype string) ProfileType {
	return UnknownProfile
}
