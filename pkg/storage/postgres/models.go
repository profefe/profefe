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

type SampleLabel struct {
	Key      string `json:"k"`
	ValueStr string `json:"s,omitempty"`
	ValueNum int64  `json:"n,omitempty"`
}

// SampleLabels is jsonb implementation for pprof.Label, pprof.NumLabel
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

type sampleRecordValuer interface {
	Value() []int64
}

type BaseSampleRecord struct {
	ServiceID uint64
	CreatedAt time.Time
	Locations pq.Int64Array
	Labels    SampleLabels
}

type SampleCPURecord struct {
	*BaseSampleRecord
	SamplesCount sql.NullInt64
	CPUNanos     sql.NullInt64
}

func (s *SampleCPURecord) Value() []int64 {
	return []int64{s.SamplesCount.Int64, s.CPUNanos.Int64}
}

type SampleHeapRecord struct {
	*BaseSampleRecord
	AllocObjects sql.NullInt64
	AllocBytes   sql.NullInt64
	InuseObjects sql.NullInt64
	InuseBytes   sql.NullInt64
}

func (s *SampleHeapRecord) Value() []int64 {
	return []int64{s.AllocObjects.Int64, s.AllocBytes.Int64, s.InuseObjects.Int64, s.InuseBytes.Int64}
}

type LocationRecord struct {
	LocationID int64
	FuncName   string
	FileName   string
	Line       int64
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
