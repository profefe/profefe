package profefe

import (
	"net/http"

	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/storage"
)

const (
	apiProfilePath = "/api/0/profile"
	apiVersionPath = "/api/0/version"
)

func RegisterRoutes(
	mux *http.ServeMux,
	logger *log.Logger,
	sr storage.Reader,
	sw storage.Writer,
) {
	mux.HandleFunc(apiVersionPath, VersionHandler)

	// XXX(narqo): everything below /api/0/ is served by profile handler
	querierSvc := NewQuerier(logger, sr)
	collectorSvc := NewCollector(logger, sw)
	mux.Handle("/api/0/", NewProfileHandler(logger, collectorSvc, querierSvc))
}
