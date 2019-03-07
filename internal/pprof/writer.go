package pprof

import (
	"fmt"
	"io"
)

type ProfileRecord struct {
	Values []int64  // sample values for this record
	Stack0 []uint64 // stack trace for this record; ends at first 0 entry

	Labels LabelSet
}

// Stack returns the stack trace associated with the record, a prefix of r.Stack0.
func (r ProfileRecord) Stack() []uint64 {
	for i, v := range r.Stack0 {
		if v == 0 {
			return r.Stack0[0:i]
		}
	}
	return r.Stack0[0:]
}

func WriteCPUProto(w io.Writer, p []ProfileRecord, locMap LocMap) error {
	if len(p) == 0 {
		return fmt.Errorf("no profile records")
	}

	b := NewProfileBuilder(w, locMap)
	b.pbValueType(tagProfile_PeriodType, "cpu", "nanoseconds")
	b.pb.int64Opt(tagProfile_Period, 0)
	b.pb.int64Opt(tagProfile_DurationNanos, 0)
	b.pbValueType(tagProfile_SampleType, "samples", "count")
	b.pbValueType(tagProfile_SampleType, "cpu", "nanoseconds")

	var locs []uint64
	for _, r := range p {
		if len(r.Values) != 2 {
			return fmt.Errorf("malformed profile record: %v", r)
		}
		buildProfileRecord(b, r, locs)
	}
	b.build()
	return nil
}

func WriteHeapProto(w io.Writer, p []ProfileRecord, locMap LocMap) error {
	if len(p) == 0 {
		return fmt.Errorf("no profile records")
	}

	b := NewProfileBuilder(w, locMap)
	b.pbValueType(tagProfile_PeriodType, "space", "bytes")
	b.pb.int64Opt(tagProfile_Period, 0)
	b.pbValueType(tagProfile_SampleType, "alloc_objects", "count")
	b.pbValueType(tagProfile_SampleType, "alloc_space", "bytes")
	b.pbValueType(tagProfile_SampleType, "inuse_objects", "count")
	b.pbValueType(tagProfile_SampleType, "inuse_space", "bytes")

	var locs []uint64
	for _, r := range p {
		if len(r.Values) != 4 {
			return fmt.Errorf("malformed profile record: %v", r)
		}
		buildProfileRecord(b, r, locs)
	}
	b.build()
	return nil
}

func buildProfileRecord(b *ProfileBuilder, r ProfileRecord, locs []uint64) {
	locs = locs[:0]
	for tries := 0; tries < 2; tries++ {
		for _, addr := range r.Stack() {
			l := b.locForPC(addr)
			if l == 0 { // runtime.goexit
				continue
			}
			locs = append(locs, l)
		}
		if len(locs) > 0 {
			break
		}
	}
	b.pbSample(r.Values, locs, func() {
		for _, label := range r.Labels {
			if label.Key != "" {
				b.pbLabel(tagSample_Label, label.Key, label.ValueStr, label.ValueNum)
			}
		}
	})
}
