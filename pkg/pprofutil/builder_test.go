package pprofutil

import (
	"testing"

	pprof "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/stretchr/testify/assert"
)

func TestProfileBuilder_IsEmpty(t *testing.T) {
	b := NewProfileBuilder(profile.CPUProfile)
	assert.True(t, b.IsEmpty())

	b.AddSample(&pprof.Sample{})
	assert.False(t, b.IsEmpty())
}
