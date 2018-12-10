package profile

import (
	"bytes"
	"io"
	"sort"
	"time"

	"github.com/google/pprof/profile"
)

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

	prof *profile.Profile
}

func New(prof *profile.Profile) *Profile {
	return NewWithMeta(prof, nil)
}

func NewWithMeta(prof *profile.Profile, meta map[string]interface{}) *Profile {
	p := &Profile{
		prof:       prof,
		ReceivedAt: time.Now().UTC(),
	}

	p.parseMeta(meta)

	if prof.TimeNanos > 0 {
		p.CreatedAt = time.Unix(0, prof.TimeNanos).UTC()
	}

	return p
}

func (p *Profile) Read(buf []byte) (int, error) {
	w := bytes.NewBuffer(buf)
	err := p.prof.Write(w)
	return w.Len(), err
}

func (p *Profile) WriteTo(w io.Writer) (int64, error) {
	err := p.prof.Write(w)
	// TODO(narqo): return proper size for io.WriterTo implementation
	return 0, err
}

func (p *Profile) parseMeta(meta map[string]interface{}) {
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
		default:
			p.Labels = append(p.Labels, Label{k, val})
		}
	}

	if len(p.Labels) > 0 {
		sort.Sort(p.Labels)
	}
}
