package api

import (
	"fmt"
	"net/url"
	"sort"

	"github.com/profefe/profefe/pkg/profile"
)

func getProfileType(q url.Values) (pt profile.ProfileType, err error) {
	if v := q.Get("type"); v != "" {
		pt = profile.ProfileTypeFromString(v)
		if pt == profile.UnknownProfile {
			err = fmt.Errorf("bad profile type %v", pt)
		}
	}
	return pt, err
}

func getLabels(q url.Values) (labels profile.Labels, err error) {
	s := q.Get("labels")

	var chunk string
	for s != "" {
		chunk, s = split2(s, ',')
		key, val := split2(chunk, '=')

		key, err = url.QueryUnescape(key)
		if err != nil {
			return nil, err
		}
		val, err = url.QueryUnescape(val)
		if err != nil {
			return nil, err
		}
		labels = append(labels, profile.Label{key, val})
	}

	sort.Sort(labels)

	return labels, nil
}

func split2(s string, ch byte) (s1, s2 string) {
	for i := 0; i < len(s); i++ {
		if s[i] == ch {
			return s[:i], s[i+1:]
		}
	}
	return s, ""
}
