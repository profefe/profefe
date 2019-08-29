package profile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestID_MarshalJSON(t *testing.T) {
	var testPID ID
	testPID.FromString("blk30gth5s5jab509hk0")

	cases := []struct {
		pid ID
	}{
		{nil},
		{NewProfileID()},
		{testPID},
	}

	for _, tc := range cases {
		b, err := tc.pid.MarshalJSON()
		require.NoError(t, err)

		var gotPid ID
		err = gotPid.UnmarshalJSON(b)
		require.NoError(t, err)

		assert.Equal(t, tc.pid, gotPid)
	}
}

func TestID_FromString(t *testing.T) {
	cases := []struct {
		pid ID
	}{
		//{nil},
		{NewProfileID()},
	}

	for _, tc := range cases {
		var gotPid ID
		err := gotPid.FromString(tc.pid.String())
		require.NoError(t, err)
		assert.Equal(t, tc.pid, gotPid)
	}
}
