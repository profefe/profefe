package postgres

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"time"

	"github.com/lib/pq"
	"github.com/lib/pq/hstore"
	"github.com/profefe/profefe/internal/pprof"
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

type BaseSampleRecord struct {
	ServiceID uint64
	CreatedAt time.Time
	Locations pq.Int64Array
	Labels    SampleLabels
}

func (s *BaseSampleRecord) toPProf(r *pprof.ProfileRecord) {
	for _, loc := range s.Locations {
		r.Stack0 = append(r.Stack0, uint64(loc))
	}
	for _, label := range s.Labels {
		if label.Key == "" {
			continue
		}
		r.Labels = append(r.Labels, pprof.Label(label))
	}
}

type SampleCPURecord struct {
	*BaseSampleRecord
	SamplesCount sql.NullInt64
	CPUNanos     sql.NullInt64
}

func (s *SampleCPURecord) ToPProf() pprof.ProfileRecord {
	r := pprof.ProfileRecord{
		Values: []int64{s.SamplesCount.Int64, s.CPUNanos.Int64},
	}
	s.toPProf(&r)
	return r
}

type SampleHeapRecord struct {
	*BaseSampleRecord
	AllocObjects sql.NullInt64
	AllocBytes   sql.NullInt64
	InuseObjects sql.NullInt64
	InuseBytes   sql.NullInt64
}

func (s *SampleHeapRecord) ToPProf() pprof.ProfileRecord {
	r := pprof.ProfileRecord{
		Values: []int64{s.AllocObjects.Int64, s.AllocBytes.Int64, s.InuseObjects.Int64, s.InuseBytes.Int64},
	}
	s.toPProf(&r)
	return r
}

type LocationRecord struct {
	LocationID uint64
	FuncName   string
	FileName   string
	Line       int
}

func (l LocationRecord) ToPProf() pprof.Location {
	return pprof.Location{l.FuncName, l.FileName, l.Line}
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
