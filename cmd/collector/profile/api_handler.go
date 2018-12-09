package profile

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
)

type APIHandler struct {
	svc *Service
}

func NewAPIHandler(svc *Service) *APIHandler {
	return &APIHandler{
		svc: svc,
	}
}

func (api *APIHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.Handle("/api/", api)
}

func (api *APIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var err error

	switch r.URL.Path {
	case "/api/v0/profiles":
		err = api.handleProfiles(w, r)
	case "/api/v0/profile":
		if r.Method == http.MethodPost {
			err = api.handleCreateProfile(w, r)
		} else {
			err = api.handleGetProfile(w, r)
		}
	case "/api/v0/profile/meta":
		if r.Method == http.MethodPost {
			err = api.handleCreateProfileMeta(w, r)
		} else {
			err = api.handleGetProfileMeta(w, r)
		}
	default:
		http.NotFound(w, r)
		return
	}

	if err == nil {
		return
	}

	ReplyError(w, err)
}

func (api *APIHandler) handleProfiles(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodGet {
		return StatusError(http.StatusMethodNotAllowed, fmt.Sprintf("bad request method: %s", r.Method), nil)
	}

	_, err := api.svc.ListProfiles(r.Context())
	if err != nil {
		return StatusError(http.StatusServiceUnavailable, "failed to list profiles", err)
	}

	ReplyOK(w)

	return nil
}

func (api *APIHandler) handleCreateProfile(w http.ResponseWriter, r *http.Request) error {
	createReq := &createProfileRequest{}
	if err := json.NewDecoder(r.Body).Decode(createReq); err != nil {
		return StatusError(http.StatusBadRequest, "bad request", fmt.Errorf("could not parse request: %v", err))
	}

	//log.Printf("request: %+v\n", createReq)

	err := api.svc.CreateProfile(r.Context(), createReq)
	if err != nil {
		return StatusError(http.StatusServiceUnavailable, "failed to create profile", err)
	}

	ReplyOK(w)

	return nil
}

func (api *APIHandler) handleGetProfile(w http.ResponseWriter, r *http.Request) error {
	getReq := &getProfileRequest{}
	if err := readGetProfileRequest(getReq, r); err != nil {
		return err
	}
	if err := getReq.Validate(); err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %s", err), err)
	}

	//log.Printf("request: %+v\n", getReq)

	p, data, err := api.svc.GetProfile(r.Context(), getReq)
	if err == storage.ErrNotFound {
		return StatusError(http.StatusNotFound, "nothing found", nil)
	} else if err != nil {
		return StatusError(http.StatusServiceUnavailable, "could not get profile", err)
	}
	defer data.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, p.Type))

	// TODO handle errors from Copy
	io.Copy(w, data)

	return nil
}

func readGetProfileRequest(in *getProfileRequest, r *http.Request) (err error) {
	if in == nil {
		*in = getProfileRequest{}
	}

	q := r.URL.Query()

	if v := q.Get("service"); v != "" {
		in.Service = v
	} else {
		return StatusError(http.StatusBadRequest, "bad request: no service", nil)
	}

	if v := q.Get("type"); v != "" {
		pt := profile.ProfileTypeFromString(v)
		if pt == profile.UnknownProfile {
			return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: bad profile type %q", v), err)
		}
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

	if v := q.Get("labels"); v != "" {
		labels, err := readLabels(v)
		if err != nil {
			return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: bad labels %q", v), err)
		}
		in.Labels = labels
	}

	return nil
}

func (api *APIHandler) handleCreateProfileMeta(w http.ResponseWriter, r *http.Request) error {
	return nil
}

func (api *APIHandler) handleGetProfileMeta(w http.ResponseWriter, r *http.Request) error {
	return nil
}
