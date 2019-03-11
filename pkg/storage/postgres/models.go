package postgres

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"time"

	"github.com/lib/pq"
	"github.com/lib/pq/hstore"
	"github.com/profefe/profefe/pkg/profile"
)

type SampleCPURecord struct {
	ServiceID    uint64
	CreatedAt    time.Time
	Locations    pq.Int64Array
	SamplesCount int64
	CPUNanos     int64
	Labels       SampleLabels
}

type SampleLabel struct {
	Key      string `json:"k"`
	ValueStr string `json:"s,omitempty"`
	ValueNum int64  `json:"n,omitempty"`
}

// SampleLabels is jsonb implementation of pprof.Label, pprof.NumLabel
type SampleLabels []SampleLabel

var (
	_ sql.Scanner   = (SampleLabels)(nil)
	_ driver.Valuer = (SampleLabels)(nil)
)

func (sl SampleLabels) Scan(src interface{}) error {
	if src == nil {
		return nil
	}
	return json.Unmarshal(src.([]byte), &sl)
}

func (sl SampleLabels) Value() (driver.Value, error) {
	if sl == nil {
		return nil, nil
	}
	return json.Marshal(sl)
}

type LocationRecord struct {
	LocationID uint64
	FuncName   string
	FileName   string
	Line       uint
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
