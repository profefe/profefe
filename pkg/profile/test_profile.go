package profile

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"

	"github.com/google/pprof/profile"
)

func NewTestProfile(t testing.TB, filename string, meta map[string]interface{}) (*Profile, io.Reader) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatalf("failed to open test profile: %v", err)
	}

	pprof, err := profile.ParseData(data)
	if err != nil {
		t.Fatalf("failed parsing profile data: %v", err)
	}

	prof := NewWithMeta(pprof, meta)
	if prof == nil {
		t.Fatal("unexpected empty profile")
	}

	return prof, bytes.NewReader(data)
}
