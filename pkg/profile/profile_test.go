package profile

import (
	"testing"

	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestID_MarshalJSON(t *testing.T) {
	cases := []struct {
		pid ID
	}{
		{nil},
		{newTestXIDProfileID()},
		{TestID},
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
	cases := []struct {
		pid ID
	}{
		{TestID},
		{newTestXIDProfileID()},
	}

	for _, tc := range cases {
		gotPid, err := IDFromString(tc.pid.String())
		require.NoError(t, err)
		assert.Equal(t, tc.pid, gotPid)
	}
}

func newTestXIDProfileID() ID {
	return xid.New().Bytes()
}
