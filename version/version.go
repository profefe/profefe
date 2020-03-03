package version

import (
	"fmt"
	"runtime"
)

var (
	version   string
	commit    string
	buildTime string
)

type Version struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildTime string `json:"build_time"`
	GoVersion string `json:"go_version"`
}

func Details() Version {
	return Version{
		Version:   version,
		Commit:    commit,
		BuildTime: buildTime,
		GoVersion: runtime.Version(),
	}
}

// String returns version details as pretty printed string.
func (v Version) String() string {
	return fmt.Sprintf(
		"profefe version %s, commit %s (%s), go version %s",
		v.Version,
		v.Commit,
		v.BuildTime,
		v.GoVersion,
	)
}
