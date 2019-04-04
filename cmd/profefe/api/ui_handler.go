package api

import (
	"fmt"
	"net/http"

	"github.com/profefe/profefe/pkg/logger"
	"github.com/profefe/profefe/pkg/profile"
)

type UIHandler struct {
	logger      *logger.Logger
	profilePepo *profile.Repository
}

func NewUIHandler(log *logger.Logger, profileRepo *profile.Repository) *APIHandler {
	return &APIHandler{
		logger:      log,
		profilePepo: profileRepo,
	}
}

func (h *UIHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.Handle("/ui/", h)
}

func (h *UIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var err error

	switch r.URL.Path {
	case "/ui/heatmap":
		err = h.handleHeatmap(w, r)
	case "/ui/flamegraph":
		err = h.handleFlamegraph(w, r)
	default:
		http.NotFound(w, r)
	}

	handleErrorHTTP(h.logger, err, w, r)
}

func (h *UIHandler) handleHeatmap(writer http.ResponseWriter, r *http.Request) error {
	return nil
}

func (h *UIHandler) handleFlamegraph(writer http.ResponseWriter, r *http.Request) error {
	req := &profile.GetProfileRequest{}
	if err := readGetProfileRequest(req, r); err != nil {
		return err
	}
	if err := req.Validate(); err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %s", err), err)
	}

	return nil
}
