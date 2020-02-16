package profefe

import (
	"fmt"
	"net/url"

	"github.com/profefe/profefe/pkg/profile"
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
