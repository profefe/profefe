package profefe

import (
	"net/http"

	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/storage"
)

const (
	apiProfilesPath      = "/api/0/profiles"
	apiProfilesMergePath = "/api/0/profiles/merge"
	apiServicesPath      = "/api/0/services"
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

	apiv0Mux := http.NewServeMux()
	apiv0Mux.HandleFunc(apiVersionPath, VersionHandler)
	apiv0Mux.Handle(apiServicesPath, NewServicesHandler(logger, querier))
	// XXX(narqo): everything else under /api/0/ is served by profiles handler
	apiv0Mux.Handle("/api/0/", NewProfilesHandler(logger, collector, querier))

	mux.Handle("/api/0/", metricsHandler(apiRequestDuration, apiv0Mux))
}
