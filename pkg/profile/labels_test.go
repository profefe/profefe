package profile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLabels_FromString(t *testing.T) {
	cases := []struct {
		in      string
		labels  Labels
		wantErr bool
	}{
		{
			"",
			nil,
			false,
		},
		{
			"blabel=value2,alabel=value1",
			Labels{{"alabel", "value1"}, {"blabel", "value2"}},
			false,
		},
		{
			"alabel=",
			Labels{{"alabel", ""}},
			false,
		},
		{
			"=value",
			nil,
			false,
		},
		{
			"alabel=val=val",
			Labels{{"alabel", "val=val"}},
			false,
		},
	}

	for _, tt := range cases {
		var labels Labels
		err := labels.FromString(tt.in)
		require.Equal(t, tt.wantErr, err != nil)
		require.Equal(t, tt.labels, labels)
	}
}

func TestLabels_String(t *testing.T) {
	cases := []struct {
		labels  Labels
		wantStr string
	}{
		{
			nil,
			"",
		},
		{
			Labels{{"alabel", "value1"}, {"blabel", "value2"}},
			"alabel=value1,blabel=value2",
		},
		{
			Labels{{"alabel", "val=val"}},
			"alabel=val=val",
		},
	}

	for _, tt := range cases {
		assert.Equal(t, tt.wantStr, tt.labels.String())
	}
}
