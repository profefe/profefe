package postgres

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"time"

	"github.com/lib/pq"
	"github.com/profefe/profefe/pkg/profile"
)

type SampleLabel struct {
	Key      string `json:"k"`
	ValueStr string `json:"s,omitempty"`
	ValueNum int64  `json:"n,omitempty"`
}

// SampleLabels is jsonb implementation for pprof.Label, pprof.NumLabel
type SampleLabels []SampleLabel

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
	LocationID int64
	Address    uint64
	Line       int64
}

type FunctionRecord struct {
	ID       int64
	FuncName string
	FileName string
}

type MappingRecord struct {
	MemStart uint64 `json:"start,omitempty"`
	MemLimit uint64 `json:"limit,omitempty"`
	Offset   uint64 `json:"offset,omitempty"`
	File     string `json:"file,omitempty"`
	BuildID  string `json:"bid,omitempty"`
}

func (m *MappingRecord) Scan(src interface{}) error {
	if src == nil {
		return nil
	}
	return json.Unmarshal(src.([]byte), &m)
}

func (m *MappingRecord) Value() (driver.Value, error) {
	if m == nil {
		return nil, nil
	}
	return json.Marshal(m)
}

type sampleRecordValuer interface {
	Value() []int64
}

type BaseSampleRecord struct {
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

type ServiceLabels profile.Labels

func (labels *ServiceLabels) Scan(src interface{}) error {
	if src == nil {
		return nil
	}

	m := make(map[string]interface{})
	if err := json.Unmarshal(src.([]byte), &m); err != nil {
		return err
	}

	*labels = ServiceLabels(profile.LabelsFromMap(m))

	return nil
}

func (labels ServiceLabels) Value() (driver.Value, error) {
	if labels == nil {
		return nil, nil
	}

	m := make(map[string]string, len(labels))
	for _, label := range labels {
		m[label.Key] = label.Value
	}

	return json.Marshal(m)
}
