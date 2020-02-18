package pprofutil

import (
	"testing"

	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/stretchr/testify/assert"
)

func TestProfileBuilder_IsEmpty(t *testing.T) {
	b := NewProfileBuilder(profile.TypeCPU)
	assert.True(t, b.IsEmpty())

	b.AddSample(&pprofProfile.Sample{})
	assert.False(t, b.IsEmpty())
}
