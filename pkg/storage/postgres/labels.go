package postgres

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"

	"github.com/lib/pq/hstore"
	"github.com/profefe/profefe/pkg/profile"
)

type sampleLabel struct {
	Key      string `json:"k"`
	ValueStr string `json:"s,omitempty"`
	ValueNum int64  `json:"n,omitempty"`
}

// sampleLabels is jsonb implementation for pprof.Label, pprof.NumLabel
type sampleLabels []sampleLabel

var (
	_ sql.Scanner   = (sampleLabels)(nil)
	_ driver.Valuer = (sampleLabels)(nil)
)

func (sl sampleLabels) Scan(src interface{}) error {
	if src == nil {
		return nil
	}
	return json.Unmarshal(src.([]byte), &sl)
}

func (sl sampleLabels) Value() (driver.Value, error) {
	if sl == nil {
		return nil, nil
	}
	return json.Marshal(sl)
}

// converts profile.Labels to hstore
// XXX implement sql/driver.Valuer in profile.Labels
func hstoreFromLabels(labels profile.Labels) hstore.Hstore {
	v := hstore.Hstore{
		Map: make(map[string]sql.NullString, len(labels)),
	}

	for _, label := range labels {
		v.Map[label.Key] = sql.NullString{String: label.Value, Valid: true}
	}

	return v
}
