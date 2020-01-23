package profefe

import (
	"net/http"

	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/storage"
)

type ServicesHandler struct {
	logger  *log.Logger
	querier *Querier
}

func NewServicesHandler(logger *log.Logger, querier *Querier) *ServicesHandler {
	return &ServicesHandler{
		logger:  logger,
		querier: querier,
	}
}

func (h *ServicesHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.URL.Path != apiServicesPath {
		HandleErrorHTTP(h.logger, ErrNotFound, w, r)
		return
	}

	services, err := h.querier.GetServices(r.Context())
	if err != nil {
		if err == storage.ErrNotFound {
			err = ErrNotFound
		}
		HandleErrorHTTP(h.logger, err, w, r)
		return
	}

	ReplyJSON(w, services)
}
