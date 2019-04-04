package ui

import (
	"github.com/profefe/profefe/internal/pprof/profile"
)

type Heatmap struct {
	Data [][]int64
}

func GetHeatmap(profs []*profile.Profile) (Heatmap, error) {
	panic("not implemented")
}
