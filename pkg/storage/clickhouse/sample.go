package clickhouse

import (
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/cespare/xxhash/v2"
	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
)

func isEmptySample(s *pprofProfile.Sample) bool {
	for _, v := range s.Value {
		if v != 0 {
			return false
		}
	}
	return true
}

// calculates a hash for a single profile sample
type samplesFingerprinter struct {
	hash *xxhash.Digest
	buf  []byte
}

var samplesFingerprinterPool = sync.Pool{
	New: func() interface{} {
		return &samplesFingerprinter{
			hash: xxhash.New(),
			buf:  make([]byte, 0, 65536), // 64KB
		}
	},
}

func (sfp *samplesFingerprinter) Fingerprint(sample *pprofProfile.Sample) uint64 {
	defer sfp.hash.Reset()

	// locations
	for _, loc := range sample.Location {
		sfp.buf = strconv.AppendUint(sfp.buf, loc.Address, 16)
		for _, line := range loc.Line {
			sfp.buf = append(sfp.buf, '|')
			sfp.buf = append(sfp.buf, line.Function.Filename...)
			sfp.buf = append(sfp.buf, ':')
			sfp.buf = strconv.AppendInt(sfp.buf, line.Line, 10)
			sfp.buf = append(sfp.buf, line.Function.Name...)
		}
	}

	sfp.hash.Write(sfp.buf)
	sfp.buf = sfp.buf[:0]

	// XXX(narqo) generally a sample has way more locations than labels,
	// thus don't bother reusing labels' buffers
	var labels []string

	// string labels
	if len(sample.Label) > 0 {
		labels = make([]string, 0, len(sample.Label))
		for k, v := range sample.Label {
			labels = append(labels, fmt.Sprintf("%q%q", k, v))
		}
		sort.Strings(labels)
		for _, label := range labels {
			sfp.hash.WriteString(label)
		}
	}

	// num labels
	if len(sample.NumLabel) > 0 {
		labels = labels[:0]
		for k, v := range sample.NumLabel {
			labels = append(labels, fmt.Sprintf("%q%x%x", k, v, sample.NumUnit[k]))
		}
		sort.Strings(labels)
		for _, label := range labels {
			sfp.hash.WriteString(label)
		}
	}

	return sfp.hash.Sum64()
}
