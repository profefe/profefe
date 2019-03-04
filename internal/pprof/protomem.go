package pprof

import (
	"io"
)

type Runtime_MemProfileRecord struct {
	AllocBytes, InUseBytes     int64      // number of bytes allocated, inuse
	AllocObjects, InUseObjects int64      // number of objects allocated, inuse
	Stack0                     [32]uint64 // stack trace for this record; ends at first 0 entry
}

// Stack returns the stack trace associated with the record,
// a prefix of r.Stack0.
func (r *Runtime_MemProfileRecord) Stack() []uint64 {
	for i, v := range r.Stack0 {
		if v == 0 {
			return r.Stack0[0:i]
		}
	}
	return r.Stack0[0:]
}

// WriteHeapProto writes the current heap profile in protobuf format to w.
func WriteHeapProto(w io.Writer, p []Runtime_MemProfileRecord, locMap map[uint64]Location) error {
	b := NewProfileBuilder(w)
	b.pbValueType(tagProfile_PeriodType, "space", "bytes")
	b.pb.int64Opt(tagProfile_Period, 0)
	b.pbValueType(tagProfile_SampleType, "alloc_objects", "count")
	b.pbValueType(tagProfile_SampleType, "alloc_space", "bytes")
	b.pbValueType(tagProfile_SampleType, "inuse_objects", "count")
	b.pbValueType(tagProfile_SampleType, "inuse_space", "bytes")

	values := []int64{0, 0, 0, 0}
	var locs []uint64
	for _, r := range p {
		locs = locs[:0]
		for tries := 0; tries < 2; tries++ {
			for _, addr := range r.Stack() {
				l := b.locForPC(addr, locMap)
				if l == 0 { // runtime.goexit
					continue
				}
				locs = append(locs, l)
			}
			if len(locs) > 0 {
				break
			}
		}

		values[0], values[1] = r.AllocObjects, r.AllocBytes
		values[2], values[3] = r.InUseObjects, r.InUseObjects
		var blockSize int64
		if r.AllocObjects > 0 {
			blockSize = r.AllocBytes / r.AllocObjects
		}
		b.pbSample(values, locs, func() {
			if blockSize != 0 {
				b.pbLabel(tagSample_Label, "bytes", "", blockSize)
			}
		})
	}
	b.build()
	return nil
}
