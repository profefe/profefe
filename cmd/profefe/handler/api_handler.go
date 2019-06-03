package handler

import (
	"fmt"
	"net/http"

	"github.com/profefe/profefe/pkg/logger"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/version"
)

type APIHandler struct {
	logger      *logger.Logger
	profilePepo *profile.Repository
}

func NewAPIHandler(log *logger.Logger, profileRepo *profile.Repository) *APIHandler {
	return &APIHandler{
		logger:      log,
		profilePepo: profileRepo,
	}
}

func (h *APIHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.Handle("/api/", h)
}

func (h *APIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var err error

	// TODO(narqo): maybe use github.com/go-chi/chi
	switch r.URL.Path {
	case "/api/0/profile":
		switch r.Method {
		case http.MethodPost:
			err = h.handleCreateProfile(w, r)
		case http.MethodGet:
			err = h.handleGetProfile(w, r)
		}
	case "/api/0/version":
		err = h.handleGetVersion(w, r)
	default:
		http.NotFound(w, r)
		return
	}

	handleErrorHTTP(h.logger, err, w, r)
}

func (h *APIHandler) handleCreateProfile(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	req := &profile.CreateProfileRequest{
		Service: q.Get("service"),
		Type:    profile.UnknownProfile,
		Labels:  nil,
	}

	iid, err := getInstanceID(q)
	if err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %v", err), nil)
	}
	req.InstanceID = iid

	ptype, err := getProfileType(q)
	if err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %v", err), nil)
	}
	req.Type = ptype

	labels, err := getLabels(q)
	if err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %v", err), nil)
	}
	req.Labels = labels

	if err := req.Validate(); err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %s", err), err)
	}

	if err := h.profilePepo.CreateProfile(r.Context(), req, r.Body); err != nil {
		return StatusError(http.StatusServiceUnavailable, "failed to create profile", err)
	}

	ReplyOK(w)

	return nil
}

func (h *APIHandler) handleGetProfile(w http.ResponseWriter, r *http.Request) error {
	req := &profile.GetProfileRequest{}
	if err := readGetProfileRequest(req, r); err != nil {
		return err
	}

	if err := req.Validate(); err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %s", err), err)
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, "profile"))

	err := h.profilePepo.GetProfileTo(r.Context(), req, w)
	if err == profile.ErrNotFound {
		return StatusError(http.StatusNotFound, "nothing found", nil)
	} else if err == profile.ErrEmpty {
		return StatusError(http.StatusNoContent, "profile empty", nil)
	}

	return err
}

func (h *APIHandler) handleGetVersion(w http.ResponseWriter, r *http.Request) error {
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

	return nil
}
