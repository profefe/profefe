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

	pprof *profile.Profile
}

func New(prof *profile.Profile) *Profile {
	return NewWithMeta(prof, nil)
}

func NewWithMeta(pprof *profile.Profile, meta map[string]interface{}) *Profile {
	prof := &Profile{
		pprof:      pprof,
		ReceivedAt: time.Now().UTC(),
	}

	prof.parseMeta(meta)

	if pprof.TimeNanos > 0 {
		prof.CreatedAt = time.Unix(0, pprof.TimeNanos).UTC()
	}

	return prof
}

func (prof *Profile) Read(buf []byte) (int, error) {
	w := bytes.NewBuffer(buf)
	err := prof.pprof.Write(w)
	return w.Len(), err
}

func (prof *Profile) WriteTo(w io.Writer) (int64, error) {
	err := prof.pprof.Write(w)
	// TODO(narqo): return proper size for io.WriterTo implementation
	return 0, err
}

func (prof *Profile) parseMeta(meta map[string]interface{}) {
	if prof.Labels == nil {
		prof.Labels = make(Labels, 0, len(meta))
	}

	for k, rawVal := range meta {
		val, _ := rawVal.(string)
		switch k {
		case LabelService:
			prof.Service = val
		case LabelID:
			prof.BuildID = val
		case LabelGeneration:
			prof.Generation = val
		case LabelType:
			prof.Type.UnmarshalString(val)
		default:
			prof.Labels = append(prof.Labels, Label{k, val})
		}
	}

	if len(prof.Labels) > 0 {
		sort.Sort(prof.Labels)
	}
}
