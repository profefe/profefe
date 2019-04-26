package pprofutil

import (
	"testing"

	pprof "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/stretchr/testify/assert"
)

func TestSampleAddLabel(t *testing.T) {
	var s *pprof.Sample

	s = &pprof.Sample{}
	SampleAddLabel(s, "key1", "bar1", 0)
	assert.NotEmpty(t, s.Label["key1"])
	assert.Empty(t, s.NumLabel)

	s = &pprof.Sample{}
	SampleAddLabel(s, "key1", "", 0)
	assert.Empty(t, s.Label)
	assert.NotEmpty(t, s.NumLabel["key1"])
}
