package handler

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/profefe/profefe/pkg/logger"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/ui"
)

type UIHandler struct {
	logger      *logger.Logger
	profilePepo *profile.Repository
}

func NewUIHandler(log *logger.Logger, profileRepo *profile.Repository) *UIHandler {
	return &UIHandler{
		logger:      log,
		profilePepo: profileRepo,
	}
}

func (h *UIHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.Handle("/ui/", h)
}

func (h *UIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var err error

	w.Header().Set("Access-Control-Allow-Origin", "*")

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

func (h *UIHandler) handleHeatmap(w http.ResponseWriter, r *http.Request) error {
	return nil
}

func (h *UIHandler) handleFlamegraph(w http.ResponseWriter, r *http.Request) error {
	req := &profile.GetProfileRequest{}
	if err := readGetProfileRequest(req, r); err != nil {
		return err
	}
	if err := req.Validate(); err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %s", err), err)
	}

	w.Header().Set("Content-Type", "application/json")

	prof, err := h.profilePepo.GetProfile(r.Context(), req)
	switch err {
	case nil:
		// continue below
	case profile.ErrNotFound, profile.ErrEmpty:
		return StatusError(http.StatusNotFound, "nothing found", nil)
	default:
		return err
	}

	fg, err := ui.GetFlamegraph(h.logger.With("svc", "ui"), prof)
	if err != nil {
		return err
	}

	if err := json.NewEncoder(w).Encode(fg); err != nil {
		return err
	}

	return nil
}
