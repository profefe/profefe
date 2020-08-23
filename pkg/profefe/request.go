package profefe

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
)

const timeFormat = "2006-01-02T15:04:05"

func parseTime(v string) (time.Time, error) {
	tm, err := time.Parse(timeFormat, v)
	if err != nil || tm.IsZero() {
		return time.Time{}, fmt.Errorf("time in unsupported format %q", v)
	}
	return tm, nil
}

func parseProfileParams(q url.Values) (service string, ptype profile.ProfileType, labels profile.Labels, err error) {
	if v := q.Get("service"); v == "" {
		return "", profile.TypeUnknown, nil, fmt.Errorf("missing \"service\"")
	} else {
		service = v
	}

	if err := ptype.FromString(q.Get("type")); err != nil {
		return "", profile.TypeUnknown, nil, fmt.Errorf("bad \"type\" %q: %s", q.Get("type"), err)
	}

	if err := labels.FromString(q.Get("labels")); err != nil {
		return "", profile.TypeUnknown, nil, fmt.Errorf("bad \"labels\" %q: %s", q.Get("labels"), err)
	}

	return service, ptype, labels, nil
}

func parseWriteProfileParams(in *storage.WriteProfileParams, r *http.Request) error {
	if in == nil {
		return fmt.Errorf("parseWriteProfileParams: nil request receiver")
	}

	q := r.URL.Query()

	service, ptype, labels, err := parseProfileParams(q)
	if err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %s", err), nil)
	}

	*in = storage.WriteProfileParams{
		Service: service,
		Type:    ptype,
		Labels:  labels,
	}

	if v := q.Get("created_at"); v != "" {
		tm, err := parseTime(v)
		if err != nil {
			return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: bad \"created_at\" %q: %s", v, err), nil)
		}
		in.CreatedAt = tm
	}

	if err := in.Validate(); err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %s", err), err)
	}

	return nil
}

func parseFindProfileParams(in *storage.FindProfilesParams, r *http.Request) (err error) {
	if in == nil {
		return errors.New("parseFindProfileParams: nil request receiver")
	}

	q := r.URL.Query()

	service, ptype, labels, err := parseProfileParams(q)
	if err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %s", err), nil)
	}

	*in = storage.FindProfilesParams{
		Service: service,
		Type:    ptype,
		Labels:  labels,
	}

	if v := q.Get("from"); v != "" {
		tm, err := parseTime(v)
		if err != nil {
			return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: bad \"from\" timestamp %q: %s", v, err), nil)
		}
		in.CreatedAtMin = tm
	} else {
		return StatusError(http.StatusBadRequest, "bad request: missing \"from\"", nil)
	}

	if v := q.Get("to"); v != "" {
		tm, err := parseTime(v)
		if err != nil {
			return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: bad \"to\" timestamp %q: %s", v, err), nil)
		}
		in.CreatedAtMax = tm
	} else {
		return StatusError(http.StatusBadRequest, "bad request: missing \"to\"", nil)
	}

	if v := q.Get("limit"); v != "" {
		l, err := strconv.Atoi(v)
		if err != nil {
			return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: bad \"limit\" %q: %s", v, err), nil)
		}
		in.Limit = l
	}

	if err := in.Validate(); err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %s", err), err)
	}

	return nil
}
