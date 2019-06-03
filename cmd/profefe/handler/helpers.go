package handler

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/profefe/profefe/pkg/logger"
	"github.com/profefe/profefe/pkg/profile"
	"golang.org/x/xerrors"
)

func readGetProfileRequest(in *profile.GetProfileRequest, r *http.Request) (err error) {
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

	if v := q.Get("limit"); v != "" {
		l, err := strconv.Atoi(v)
		if err != nil {
			return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: bad limit %q", v), err)
		}
		in.Limit = l
	}

	return nil
}

func getProfileType(q url.Values) (ptype profile.ProfileType, err error) {
	if v := q.Get("type"); v != "" {
		if err := ptype.FromString(v); err != nil {
			return ptype, err
		}
		if ptype == profile.UnknownProfile {
			err = fmt.Errorf("bad profile type %v", ptype)
		}
	}
	return ptype, err
}

func getInstanceID(q url.Values) (iid profile.InstanceID, err error) {
	if v := q.Get("instance_id"); v != "" {
		return profile.InstanceID(v), nil
	}
	return iid, fmt.Errorf("bad request: bad instance id %q", q.Get("instance_id"))
}

func getLabels(q url.Values) (labels profile.Labels, err error) {
	err = labels.FromString(q.Get("labels"))
	return labels, err
}

func handleErrorHTTP(logger *logger.Logger, err error, w http.ResponseWriter, r *http.Request) {
	if err == nil {
		return
	}

	ReplyError(w, err)

	if origErr := xerrors.Unwrap(err); origErr != nil {
		err = origErr
	}
	if err != nil {
		logger.Errorw("request failed", "url", r.URL.String(), "err", err)
	}
}
