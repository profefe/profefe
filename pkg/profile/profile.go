package profile

import (
	"fmt"
	"strings"
	"time"
)

const TestID = ID("bpc00mript33iv4net00")

type ID string

func JoinIDs(ids ...ID) (string, error) {
	if len(ids) == 0 {
		return "", nil
	}
	var buf strings.Builder
	for n, id := range ids {
		if n > 0 {
			buf.WriteByte('+')
		}
		sid := string(id)
		if strings.ContainsRune(sid, '+') {
			return "", fmt.Errorf("could not join %v: found recerved char in %q", ids, id)
		}
		buf.WriteString(sid)
	}
	return buf.String(), nil
}

func SplitIDs(s string) ([]ID, error) {
	if s == "" {
		return nil, nil
	}
	ss := strings.Split(s, "+")
	ids := make([]ID, len(ss))
	for i, sid := range ss {
		if sid == "" {
			return nil, fmt.Errorf("could not split %q: found empty id at %d", s, i)
		}
		ids[i] = ID(sid)
	}
	return ids, nil
}

type Meta struct {
	ProfileID ID          `json:"profile_id"`
	Service   string      `json:"service"`
	Type      ProfileType `json:"type"`
	Labels    Labels      `json:"labels,omitempty"`
	CreatedAt time.Time   `json:"created_at,omitempty"`
}
