package postgres

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
)

type sampleLabel struct {
	Key      string `json:"key"`
	ValueStr string `json:"value_str,omitempty"`
	ValueNum int64  `json:"value_num,omitempty"`
}

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
