package pprofutil

import (
	pprof "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/profile"
)

type ProfileBuilder struct {
	ptyp profile.ProfileType
	prof *pprof.Profile
}

func NewProfileBuilder(ptyp profile.ProfileType) *ProfileBuilder {
	return &ProfileBuilder{
		ptyp: ptyp,
		prof: &pprof.Profile{},
	}
}

func (pb *ProfileBuilder) IsEmpty() bool {
	return len(pb.prof.Sample) == 0
}

func (pb *ProfileBuilder) AddSample(s *pprof.Sample) {
	pb.prof.Sample = append(pb.prof.Sample, s)
}

// TODO(narqo) See profefe/profefe#1.
func (pb *ProfileBuilder) AddMapping(m *pprof.Mapping) {
	panic("not implemented")
}

func (pb *ProfileBuilder) AddLocation(loc *pprof.Location) {
	if loc.ID == 0 {
		loc.ID = nextID(len(pb.prof.Location))
	}
	pb.prof.Location = append(pb.prof.Location, loc)
}

func (pb *ProfileBuilder) AddFunction(fn *pprof.Function) {
	if fn.ID == 0 {
		fn.ID = nextID(len(pb.prof.Function))
	}
	pb.prof.Function = append(pb.prof.Function, fn)
}

func (pb *ProfileBuilder) Build() (*pprof.Profile, error) {
	switch pb.ptyp {
	case profile.CPUProfile:
		pb.buildCPU()
	case profile.HeapProfile:
		pb.buildHeap()
	}

	err := pb.prof.CheckValid()

	return pb.prof, err
}

func (pb *ProfileBuilder) buildCPU() {
	pb.prof.SampleType = []*pprof.ValueType{
		{Type: "samples", Unit: "count"},
		{Type: "cpu", Unit: "nanoseconds"},
	}
	pb.prof.PeriodType = &pprof.ValueType{
		Type: "cpu",
		Unit: "nanoseconds",
	}
}

func (pb *ProfileBuilder) buildHeap() {
	pb.prof.SampleType = []*pprof.ValueType{
		{Type: "alloc_objects", Unit: "count"},
		{Type: "alloc_space", Unit: "bytes"},
		{Type: "inuse_objects", Unit: "count"},
		{Type: "inuse_space", Unit: "bytes"},
	}
	pb.prof.PeriodType = &pprof.ValueType{
		Type: "space",
		Unit: "bytes",
	}
}

func nextID(n int) uint64 {
	return uint64(1 + n)
}
