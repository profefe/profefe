package pprofutil

import (
	pprof "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/profile"
)

type ProfileBuilders struct {
	ptyp          profile.ProfileType
	buildersByPid map[int64]*ProfileBuilder

	locSet     map[int64]*pprof.Location
	locIDToPid map[int64]int64
}

func NewProfileBuilders(ptyp profile.ProfileType) *ProfileBuilders {
	return &ProfileBuilders{
		ptyp:          ptyp,
		buildersByPid: make(map[int64]*ProfileBuilder),

		locSet:     make(map[int64]*pprof.Location),
		locIDToPid: make(map[int64]int64),
	}
}

func (pbs *ProfileBuilders) ProfileBuilder(pid int64) (pb *ProfileBuilder) {
	if pb, _ = pbs.buildersByPid[pid]; pb == nil {
		pb = NewProfileBuilder(pbs.ptyp)
		pbs.buildersByPid[pid] = pb
	}
	return pb
}

func (pbs *ProfileBuilders) Sample(pid int64, val []int64) (sample *pprof.Sample) {
	s := &pprof.Sample{
		Value: val,
	}
	pbs.ProfileBuilder(pid).AddSample(s)
	return s
}

func (pbs *ProfileBuilders) Location(pid, locID int64) (loc *pprof.Location) {
	if loc, _ = pbs.locSet[locID]; loc == nil {
		loc = &pprof.Location{}
		pbs.locSet[locID] = loc

		pbs.ProfileBuilder(pid).AddLocation(loc)
		pbs.locIDToPid[locID] = pid
	}
	return loc
}

func (pbs *ProfileBuilders) IsEmpty() bool {
	if len(pbs.buildersByPid) == 0 {
		return true
	}

	for _, pb := range pbs.buildersByPid {
		if !pb.IsEmpty() {
			return false
		}
	}

	return true
}

func (pbs *ProfileBuilders) BuildAll() ([]*pprof.Profile, error) {
	profs := make([]*pprof.Profile, 0, len(pbs.buildersByPid))
	for _, pb := range pbs.buildersByPid {
		prof, err := pb.Build()
		if err != nil {
			return nil, err
		}
		profs = append(profs, prof)
	}
	return profs, nil
}

func BuilderByLocationID(pbs *ProfileBuilders, locID int64) (pb *ProfileBuilder) {
	pid := pbs.locIDToPid[locID]
	return pbs.buildersByPid[pid]
}

func LocationByLocationID(pbs *ProfileBuilders, locID int64) *pprof.Location {
	return pbs.locSet[locID]
}

func LocationIDs(pbs *ProfileBuilders) []int64 {
	locIDs := make([]int64, 0, len(pbs.locSet))
	for locID := range pbs.locSet {
		locIDs = append(locIDs, locID)
	}
	return locIDs
}
