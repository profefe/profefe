package version

import (
	"fmt"
	"runtime"
)

var (
	Version   string
	Commit    string
	BuildTime string
)

// String returns version details as pretty printed string.
func String() string {
	return fmt.Sprintf(
		"profefe version %s, commit %s (%s), go version %s",
		Version,
		Commit,
		BuildTime,
		runtime.Version(),
	)
}
