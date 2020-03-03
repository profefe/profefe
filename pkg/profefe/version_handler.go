package profefe

import (
	"net/http"

	"github.com/profefe/profefe/version"
)

func VersionHandler(w http.ResponseWriter, _ *http.Request) {
	ReplyJSON(w, version.Details())
}
