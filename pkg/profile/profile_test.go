package profile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestID_MarshalJSON(t *testing.T) {
	testPID, _ := IDFromString("blk30gth5s5jab509hk0")

	cases := []struct {
		pid ID
	}{
		{nil},
		{NewID()},
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

func TestIDFromString(t *testing.T) {
	testPID, _ := IDFromString("blk30gth5s5jab509hk0")

	cases := []struct {
		pid ID
	}{
		{testPID},
		{NewID()},
	}

	for _, tc := range cases {
		gotPid, err := IDFromString(tc.pid.String())
		require.NoError(t, err)
		assert.Equal(t, tc.pid, gotPid)
	}
}
