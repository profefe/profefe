package profefe

import (
	"net/http"

	"github.com/profefe/profefe/version"
)

func VersionHandler(w http.ResponseWriter, r *http.Request) {
	resp := struct {
		Version   string `json:"version"`
		Commit    string `json:"commit"`
		BuildTime string `json:"build_time"`
	}{
		Version:   version.Version,
		Commit:    version.Commit,
		BuildTime: version.BuildTime,
	}

	ReplyJSON(w, resp)
}
