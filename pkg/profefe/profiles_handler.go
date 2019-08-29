package profefe

import (
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
	"golang.org/x/xerrors"
)

type ProfilesHandler struct {
	logger    *log.Logger
	collector *Collector
	querier   *Querier
}

func NewProfilesHandler(logger *log.Logger, collector *Collector, querier *Querier) *ProfilesHandler {
	return &ProfilesHandler{
		logger:    logger,
		collector: collector,
		querier:   querier,
	}
}

func (h *ProfilesHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var err error

	if p := path.Clean(r.URL.Path); p == apiProfilesPath {
		switch r.Method {
		case http.MethodPost:
			err = h.HandleCreateProfile(w, r)
		case http.MethodGet:
			err = h.HandleFindProfiles(w, r)
		}
	} else if len(p) > len(apiProfilesPath) {
		err = h.HandleGetProfile(w, r)
	}

	HandleErrorHTTP(h.logger, err, w, r)
}

func (h *ProfilesHandler) HandleCreateProfile(w http.ResponseWriter, r *http.Request) error {
	req := &WriteProfileRequest{}
	if err := req.UnmarshalURL(r.URL.Query()); err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %v", err), nil)
	}

	err := h.collector.CollectProfileFrom(r.Context(), r.Body, req)
	if err != nil {
		return StatusError(http.StatusInternalServerError, "failed to create profile", err)
	}

	ReplyOK(w)

	return nil
}

func (h *ProfilesHandler) HandleGetProfile(w http.ResponseWriter, r *http.Request) error {
	rawPid := r.URL.Path[len(apiProfilesPath):] // id part of the path
	rawPid = strings.Trim(rawPid, "/")
	if rawPid == "" {
		return StatusError(http.StatusBadRequest, "no profile id", nil)
	}

	// special case: find profiles and merge them into one
	if rawPid == "merge" {
		return h.HandleFindProfile(w, r)
	}

	var pid profile.ID
	if err := pid.FromString(rawPid); err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad profile id %q", rawPid), err)
	}

	pp, err := h.querier.GetProfile(r.Context(), pid)
	if err == storage.ErrNotFound {
		return StatusError(http.StatusNotFound, "nothing found", nil)
	} else if err != nil {
		return xerrors.Errorf("could not get profile by id %v: %w", pid, err)
	}

	w.Header().Set("Content-Type", "application/octet-stream")

	return pp.Write(w)
}

func (h *ProfilesHandler) HandleFindProfiles(w http.ResponseWriter, r *http.Request) error {
	params := &storage.FindProfilesParams{}
	if err := parseFindProfileParams(params, r); err != nil {
		return err
	}

	w.Header().Set("Content-Type", "application/json")

	metas, err := h.querier.FindProfiles(r.Context(), params)
	if err == storage.ErrNotFound {
		return StatusError(http.StatusNotFound, "nothing found", nil)
	} else if err == storage.ErrEmpty {
		return StatusError(http.StatusNoContent, "profile empty", nil)
	}

	ReplyJSON(w, metas)

	return nil
}

func (h *ProfilesHandler) HandleFindProfile(w http.ResponseWriter, r *http.Request) error {
	params := &storage.FindProfilesParams{}
	if err := parseFindProfileParams(params, r); err != nil {
		return err
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, params.Type))

	err := h.querier.FindProfileTo(r.Context(), w, params)
	if err == storage.ErrNotFound {
		return StatusError(http.StatusNotFound, "nothing found", nil)
	} else if err == storage.ErrEmpty {
		return StatusError(http.StatusNoContent, "profile empty", nil)
	}
	return err
}

func parseFindProfileParams(in *storage.FindProfilesParams, r *http.Request) (err error) {
	if in == nil {
		return xerrors.New("parseFindProfileParams: nil request receiver")
	}

	q := r.URL.Query()

	if v := q.Get("service"); v != "" {
		in.Service = v
	} else {
		return StatusError(http.StatusBadRequest, "bad request: missing service", nil)
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
		in.CreatedAtMin = tm
	}

	if v := q.Get("to"); v != "" {
		tm, err := time.Parse(timeFormat, v)
		if err != nil || tm.IsZero() {
			return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: bad to %q", v), err)
		}
		in.CreatedAtMax = tm
	}

	if labels, err := getLabels(q); err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: bad labels %q", q.Get("labels")), err)
	} else {
		in.Labels = labels
	}

	if v := q.Get("limit"); v != "" {
		l, err := strconv.Atoi(v)
		if err != nil {
			return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: bad limit %q", v), err)
		}
		in.Limit = l
	}

	if err := in.Validate(); err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %s", err), err)
	}

	return nil
}
