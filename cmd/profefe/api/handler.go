package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

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

func (api *APIHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.Handle("/api/", api)
}

func (api *APIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var err error

	// TODO(narqo): maybe use github.com/go-chi/chi
	switch r.URL.Path {
	case "/api/0/profiles":
		err = api.handleGetProfiles(w, r)
	case "/api/0/profile":
		switch r.Method {
		case http.MethodPut:
			err = api.handleCreateProfile(w, r)
		case http.MethodPost:
			err = api.handleUpdateProfile(w, r)
		case http.MethodGet:
			err = api.handleGetProfile(w, r)
		}
	case "/api/0/version":
		err = api.handleGetVersion(w, r)
	default:
		http.NotFound(w, r)
		return
	}

	if err == nil {
		return
	}

	ReplyError(w, err)

	if origErr, _ := err.(causer); origErr != nil {
		err = origErr.Cause()
	}
	if err != nil {
		api.logger.Errorw("request failed", "url", r.URL.String(), "err", err)
	}
}

func (api *APIHandler) handleGetProfiles(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodGet {
		return StatusError(http.StatusMethodNotAllowed, fmt.Sprintf("bad request method: %s", r.Method), nil)
	}
	return StatusError(http.StatusMethodNotAllowed, "not implemented", nil)
}

func (api *APIHandler) handleCreateProfile(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	req := &profile.CreateProfileRequest{
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

	token, err := api.profilePepo.CreateProfile(r.Context(), req)
	if err != nil {
		return StatusError(http.StatusServiceUnavailable, "failed to create profile", err)
	}

	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Content-Type", "application/json")

	resp := struct {
		Code  int    `json:"code"`
		Token string `json:"token"`
	}{
		Code:  http.StatusCreated,
		Token: token,
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		return err
	}
	return nil
}

func (api *APIHandler) handleUpdateProfile(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	req := &profile.UpdateProfileRequest{
		ID:    q.Get("id"),
		Token: q.Get("token"),
		Type:  profile.UnknownProfile,
	}
	pt, err := getProfileType(q)
	if err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %v", err), nil)
	}
	req.Type = pt

	if err := api.profilePepo.UpdateProfile(r.Context(), req, r.Body); err != nil {
		return StatusError(http.StatusServiceUnavailable, "failed to create profile", err)
	}

	ReplyOK(w)

	return nil
}

func (api *APIHandler) handleGetProfile(w http.ResponseWriter, r *http.Request) error {
	req := &profile.GetProfileRequest{}
	if err := readGetProfileRequest(req, r); err != nil {
		return err
	}
	if err := req.Validate(); err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %s", err), err)
	}

	api.logger.Debugf("req %+v", req)

	prof, profReader, err := api.profilePepo.GetProfile(r.Context(), req)
	if err == profile.ErrNotFound {
		return StatusError(http.StatusNotFound, "nothing found", nil)
	} else if err != nil {
		return StatusError(http.StatusServiceUnavailable, "could not get profile", err)
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, prof.Type))

	_, err = io.Copy(w, profReader)
	if err != nil {
		err = StatusError(http.StatusServiceUnavailable, "could not write profile response", err)
	}
	return err
}

func readGetProfileRequest(in *profile.GetProfileRequest, r *http.Request) (err error) {
	if in == nil {
		*in = profile.GetProfileRequest{}
	}

	q := r.URL.Query()

	if v := q.Get("service"); v != "" {
		in.Service = v
	} else {
		return StatusError(http.StatusBadRequest, "bad request: no service", nil)
	}

	if pt, err := getProfileType(q); err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: bad profile type %q", q.Get("type")), err)
	} else {
		in.Type = pt
	}

	timeFormat := "2006-01-02T15:04:05"

	if v := q.Get("from"); v != "" {
		tm, err := time.Parse(timeFormat, v)
		if err != nil || tm.IsZero() {
			return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: bad from %q", v), err)
		}
		in.From = tm
	}

	if v := q.Get("to"); v != "" {
		tm, err := time.Parse(timeFormat, v)
		if err != nil || tm.IsZero() {
			return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: bad to %q", v), err)
		}
		in.To = tm
	}

	if labels, err := getLabels(q); err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: bad labels %q", q.Get("labels")), err)
	} else {
		in.Labels = labels
	}

	return nil
}

func (api *APIHandler) handleGetVersion(w http.ResponseWriter, r *http.Request) error {
	w.Header().Set("Content-Type", "application/json")

	resp := struct {
		Version   string `json:"version"`
		Commit    string `json:"commit"`
		BuildTime string `json:"build_time"`
	}{
		Version:   version.Version,
		Commit:    version.Commit,
		BuildTime: version.BuildTime,
	}
	return json.NewEncoder(w).Encode(resp)
}
