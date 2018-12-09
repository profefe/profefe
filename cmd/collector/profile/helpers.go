package profile

import (
	"net/url"
	"sort"

	"github.com/profefe/profefe/pkg/profile"
)

func readLabels(s string) (labels profile.Labels, err error) {
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
