package profefe

import (
	"net/http"

	"github.com/profefe/profefe/pkg/log"
	"github.com/prometheus/client_golang/prometheus"
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
	registry prometheus.Registerer,
	collector *Collector,
	querier *Querier,
) {
	apiv0Mux := http.NewServeMux()
	apiv0Mux.HandleFunc(apiVersionPath, VersionHandler)
	apiv0Mux.Handle(apiServicesPath, NewServicesHandler(logger, querier))
	// XXX(narqo): everything else under /api/0/ is served by profiles handler
	apiv0Mux.Handle("/api/0/", NewProfilesHandler(logger, collector, querier))

	mux.Handle("/api/0/", metricsHandler(registry, apiv0Mux))
}
