package pprofutil

import (
	pprof "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/profile"
)

type ProfilesBuilder struct {
	ptyp profile.ProfileType

	buildersByPid map[int64]*ProfileBuilder
	locsByLocID   map[int64]*pprof.Location
	// location id -> profile id
	locIDToPid map[int64]int64
}

func NewProfilesBuilder(ptyp profile.ProfileType) *ProfilesBuilder {
	return &ProfilesBuilder{
		ptyp:          ptyp,
		buildersByPid: make(map[int64]*ProfileBuilder),

		locsByLocID: make(map[int64]*pprof.Location),
		locIDToPid:  make(map[int64]int64),
	}
}

// ProfileBuilder returns an existing ProfileBuilder by pid or creates a new one.
func (psb *ProfilesBuilder) ProfileBuilder(pid int64) (pb *ProfileBuilder) {
	if pb, _ = psb.buildersByPid[pid]; pb == nil {
		pb = NewProfileBuilder(psb.ptyp)
		psb.buildersByPid[pid] = pb
	}
	return pb
}

func (psb *ProfilesBuilder) ProfileBuilderByLocation(locID int64) (pb *ProfileBuilder) {
	pid, ok := psb.locIDToPid[locID]
	if !ok {
		return nil
	}
	return psb.buildersByPid[pid]
}

func (psb *ProfilesBuilder) Location(pid, locID int64) (loc *pprof.Location) {
	if loc, _ = psb.locsByLocID[locID]; loc == nil {
		loc = &pprof.Location{}
		psb.locsByLocID[locID] = loc

		psb.ProfileBuilder(pid).AddLocation(loc)
		psb.locIDToPid[locID] = pid
	}
	return loc
}

func (psb *ProfilesBuilder) LocationByID(locID int64) *pprof.Location {
	return psb.locsByLocID[locID]
}

func (psb *ProfilesBuilder) IsEmpty() bool {
	if len(psb.buildersByPid) == 0 {
		return true
	}

	for _, pb := range psb.buildersByPid {
		if !pb.IsEmpty() {
			return false
		}
	}

	return true
}

func (psb *ProfilesBuilder) BuildAll() ([]*pprof.Profile, error) {
	profs := make([]*pprof.Profile, 0, len(psb.buildersByPid))
	for _, pb := range psb.buildersByPid {
		prof, err := pb.Build()
		if err != nil {
			return nil, err
		}
		profs = append(profs, prof)
	}
	return profs, nil
}

func LocationIDs(pbs *ProfilesBuilder) []int64 {
	locIDs := make([]int64, 0, len(pbs.locsByLocID))
	for locID := range pbs.locsByLocID {
		locIDs = append(locIDs, locID)
	}
	return locIDs
}
