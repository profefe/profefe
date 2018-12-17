package profile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProfileTypeFromString(t *testing.T) {
	cases := []struct {
		in   string
		want ProfileType
	}{
		{"cpu", CPUProfile},
		{"blah", UnknownProfile},
	}

	for _, tc := range cases {
		assert.Equal(t, tc.want, ProfileTypeFromString(tc.in))
	}
}

func TestProfileType_MarshalUnmarshalString(t *testing.T) {
	cases := []ProfileType{
		CPUProfile,
		UnknownProfile,
	}

	for _, want := range cases {
		var got ProfileType
		require.NoError(t, got.UnmarshalString(want.MarshalString()))
		assert.Equalf(t, want, got, "type %v (%v)", want.String(), want.MarshalString())
	}
}
