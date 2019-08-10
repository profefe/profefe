package profefe

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
	"golang.org/x/xerrors"
)

type ProfileHandler struct {
	logger *log.Logger
	st     storage.Storage
}

func NewProfileHandler(logger *log.Logger, st storage.Storage) *ProfileHandler {
	return &ProfileHandler{
		logger: logger,
		st:     st,
	}
}

func (h *ProfileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var err error

	switch r.Method {
	case http.MethodPost:
		err = h.HandleCreateProfile(w, r)
	case http.MethodGet:
		err = h.HandlerGetProfile(w, r)
	}

	HandleErrorHTTP(h.logger, err, w, r)
}

func (h *ProfileHandler) HandleCreateProfile(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	req := &storage.WriteProfileRequest{
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
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %v", err), err)
	}

	err = storage.WriteProfileFrom(r.Context(), r.Body, h.st, req)
	if err != nil {
		return StatusError(http.StatusInternalServerError, "failed to create profile", err)
	}

	ReplyOK(w)

	return nil
}

func (h *ProfileHandler) HandlerGetProfile(w http.ResponseWriter, r *http.Request) error {
	req := &storage.FindProfileRequest{}
	if err := readFindProfileRequest(req, r); err != nil {
		return err
	}

	if err := req.Validate(); err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %s", err), err)
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, "profile"))

	err := storage.FindProfileTo(r.Context(), w, h.st, req)
	if err == storage.ErrNotFound {
		return StatusError(http.StatusNotFound, "nothing found", nil)
	} else if err == storage.ErrEmpty {
		return StatusError(http.StatusNoContent, "profile empty", nil)
	}

	return err
}

func readFindProfileRequest(in *storage.FindProfileRequest, r *http.Request) (err error) {
	if in == nil {
		return xerrors.New("readGetProfileRequest: nil request receiver")
	}

	q := r.URL.Query()

	if v := q.Get("service"); v != "" {
		in.Service = v
	} else {
		return StatusError(http.StatusBadRequest, "bad request: missing name", nil)
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

	return nil
}
