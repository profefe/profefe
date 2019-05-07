package pprofutil

import (
	"testing"

	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/stretchr/testify/assert"
)

func TestSampleAddLabel(t *testing.T) {
	var s *pprofProfile.Sample

	s = &pprofProfile.Sample{}
	SampleAddLabel(s, "key1", "bar1", 0)
	assert.NotEmpty(t, s.Label["key1"])
	assert.Empty(t, s.NumLabel)

	s = &pprofProfile.Sample{}
	SampleAddLabel(s, "key1", "", 0)
	assert.Empty(t, s.Label)
	assert.NotEmpty(t, s.NumLabel["key1"])
}
