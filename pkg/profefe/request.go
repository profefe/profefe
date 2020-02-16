package profefe

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
	"golang.org/x/xerrors"
)

func getProfileType(q url.Values) (ptype profile.ProfileType, err error) {
	if v := q.Get("type"); v != "" {
		if err := ptype.FromString(v); err != nil {
			return ptype, err
		}
		if ptype == profile.TypeUnknown {
			err = fmt.Errorf("bad profile type %v", ptype)
		}
	}
	return ptype, err
}

func getLabels(q url.Values) (labels profile.Labels, err error) {
	err = labels.FromString(q.Get("labels"))
	return labels, err
}

const timeFormat = "2006-01-02T15:04:05"

func parseTime(v string) (time.Time, error) {
	tm, err := time.Parse(timeFormat, v)
	if err != nil || tm.IsZero() {
		return time.Time{}, xerrors.Errorf("time in unsupported format %q", v)
	}
	return tm, nil
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
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: bad profile type %q: %s", q.Get("type"), err), nil)
	} else {
		in.Type = pt
	}

	if v := q.Get("from"); v != "" {
		tm, err := parseTime(v)
		if err != nil {
			return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: bad \"from\" timestamp %q: %s", v, err), nil)
		}
		in.CreatedAtMin = tm
	}

	if v := q.Get("to"); v != "" {
		tm, err := parseTime(v)
		if err != nil {
			return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: bad \"to\" timestamp %q: %s", v, err), nil)
		}
		in.CreatedAtMax = tm
	}

	if labels, err := getLabels(q); err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: bad labels %q: %s", q.Get("labels"), err), nil)
	} else {
		in.Labels = labels
	}

	if v := q.Get("limit"); v != "" {
		l, err := strconv.Atoi(v)
		if err != nil {
			return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: bad limit %q: %s", v, err), nil)
		}
		in.Limit = l
	}

	if err := in.Validate(); err != nil {
		return StatusError(http.StatusBadRequest, fmt.Sprintf("bad request: %s", err), err)
	}

	return nil
}
