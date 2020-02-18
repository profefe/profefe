package profile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProfileType_FromString(t *testing.T) {
	cases := []struct {
		in   string
		want ProfileType
	}{
		{"cpu", TypeCPU},
		{"heap", TypeHeap},
		{"threadcreate", TypeThreadcreate},
		{"other", TypeOther},
		{"trace", TypeTrace},
		{"blah", TypeUnknown},
	}

	for _, tc := range cases {
		var pt ProfileType
		require.NoError(t, pt.FromString(tc.in))
		assert.Equal(t, tc.want, pt)
	}
}
