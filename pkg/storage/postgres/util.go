package postgres

import (
	"database/sql"
	"sort"

	"github.com/lib/pq/hstore"
	"github.com/profefe/profefe/pkg/profile"
)

// TODO implement sql.Valuer in profile.Labels
func hstoreFromLabels(labels profile.Labels) hstore.Hstore {
	v := hstore.Hstore{
		Map: make(map[string]sql.NullString, len(labels)),
	}

	for _, label := range labels {
		v.Map[label.Key] = sql.NullString{String: label.Value, Valid: true}
	}

	return v
}

// TODO implement sql.Scanner in profile.Labels
func hstoreToLabes(h hstore.Hstore, labels profile.Labels) profile.Labels {
	if len(h.Map) == 0 {
		return labels
	}

	for k, v := range h.Map {
		if !v.Valid {
			continue
		}
		labels = append(labels, profile.Label{k, v.String})
	}

	sort.Sort(labels)

	return labels
}
