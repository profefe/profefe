package profefe

import (
	"net/http"

	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/storage"
)

const (
	apiProfilesPath      = "/api/0/profiles"
	apiProfilesMergePath = "/api/0/profiles/merge"
	apiVersionPath       = "/api/0/version"
)

func SetupRoutes(
	mux *http.ServeMux,
	logger *log.Logger,
	sr storage.Reader,
	sw storage.Writer,
) {
	querier := NewQuerier(logger, sr)
	collector := NewCollector(logger, sw)

	mux.HandleFunc(apiVersionPath, VersionHandler)

	// XXX(narqo): everything below /api/0/ is served by profiles handler
	mux.Handle("/api/0/", NewProfilesHandler(logger, collector, querier))
}
