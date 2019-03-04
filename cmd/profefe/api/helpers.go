package api

import (
	"fmt"
	"net/url"

	"github.com/profefe/profefe/pkg/profile"
)

func getProfileType(q url.Values) (pt profile.ProfileType, err error) {
	if v := q.Get("type"); v != "" {
		if err := pt.FromString(v); err != nil {
			return pt, err
		}
		if pt == profile.UnknownProfile {
			err = fmt.Errorf("bad profile type %v", pt)
		}
	}
	return pt, err
}

func getLabels(q url.Values) (labels profile.Labels, err error) {
	err = labels.FromString(q.Get("labels"))
	return labels, err
}
