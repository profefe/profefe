package handler

import (
	"context"
	"fmt"
	"io"
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
	case "/api/0/services":
		err = h.handleGetServices(w, r)
	case "/api/0/profiles":
		err = h.handleGetProfiles(w, r)
	case "/api/0/profile":
		switch r.Method {
		case http.MethodPut:
			err = h.handleCreateProfile(w, r)
		case http.MethodPost:
			err = h.handleUpdateProfile(w, r)
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

func (h *APIHandler) handleGetServices(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodGet {
		return StatusError(http.StatusMethodNotAllowed, fmt.Sprintf("bad request method: %s", r.Method), nil)
	}

	q := r.URL.Query()
	req := &profile.GetServicesRequest{
		Service: q.Get("service"),
	}
	services, err := h.profilePepo.GetServices(r.Context(), req)
	if err == profile.ErrNotFound {
		return StatusError(http.StatusNotFound, "nothing found", nil)
	} else if err != nil {
		return err
	}

	ReplyJSON(w, services)

	return nil
}

func (h *APIHandler) handleGetProfiles(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodGet {
		return StatusError(http.StatusMethodNotAllowed, fmt.Sprintf("bad request method: %s", r.Method), nil)
	}

	req := &profile.GetProfilesRequest{}
	if err := readGetProfilesRequest(req, r); err != nil {
		return err
	}
	if err := req.Validate(); err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %s", err), err)
	}

	return handleGetProfiles(r.Context(), w, req, h.profilePepo.GetProfilesTo)
}

func (h *APIHandler) handleCreateProfile(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	req := &profile.CreateServiceRequest{
		ID:      q.Get("id"),
		Service: q.Get("service"),
		Labels:  nil,
	}
	labels, err := getLabels(q)
	if err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %v", err), nil)
	}
	req.Labels = labels

	if err := req.Validate(); err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %s", err), err)
	}

	token, err := h.profilePepo.CreateService(r.Context(), req)
	if err != nil {
		return StatusError(http.StatusServiceUnavailable, "failed to create profile", err)
	}

	w.WriteHeader(http.StatusCreated)

	resp := struct {
		Code  int    `json:"code"`
		Token string `json:"token"`
	}{
		Code:  http.StatusCreated,
		Token: token,
	}

	ReplyJSON(w, resp)

	return nil
}

func (h *APIHandler) handleUpdateProfile(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	req := &profile.CreateProfileRequest{
		ID:    q.Get("id"),
		Token: q.Get("token"),
		Type:  profile.UnknownProfile,
	}
	pt, err := getProfileType(q)
	if err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %v", err), nil)
	}
	req.Type = pt

	if err := h.profilePepo.CreateProfile(r.Context(), req, r.Body); err != nil {
		return StatusError(http.StatusServiceUnavailable, "failed to update profile", err)
	}

	ReplyOK(w)

	return nil
}

func (h *APIHandler) handleGetProfile(w http.ResponseWriter, r *http.Request) error {
	req := &profile.GetProfilesRequest{}
	if err := readGetProfilesRequest(req, r); err != nil {
		return err
	}
	if err := req.Validate(); err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %s", err), err)
	}

	return handleGetProfiles(r.Context(), w, req, h.profilePepo.GetProfileTo)
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

func handleGetProfiles(
	ctx context.Context,
	w http.ResponseWriter,
	req *profile.GetProfilesRequest,
	getProfileToFunc func(ctx context.Context, req *profile.GetProfilesRequest, w io.Writer) error,
) error {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, "profile"))

	err := getProfileToFunc(ctx, req, w)
	if err == profile.ErrNotFound {
		return StatusError(http.StatusNotFound, "nothing found", nil)
	} else if err == profile.ErrEmpty {
		return StatusError(http.StatusNoContent, "profile empty", nil)
	}
	return err
}
