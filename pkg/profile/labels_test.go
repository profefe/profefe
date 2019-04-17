package profile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLabels_Equal(t *testing.T) {
	cases := []struct {
		labels1   Labels
		labels2   Labels
		wantEqual bool
	}{
		{nil, nil, true},
		{nil, Labels{{"alabel", "value1"}}, false},
		{
			Labels{{"alabel", "value1"}},
			Labels{{"alabel", "value1"}, {"blabel", "value2"}},
			false,
		},
		{
			Labels{{"alabel", "value1"}},
			Labels{{"alabel", "value2"}},
			false,
		},
		{
			Labels{{"alabel", "value1"}},
			Labels{{"blabel", "value1"}},
			false,
		},
		{
			Labels{{"alabel", "value1"}},
			Labels{{"alabel", "value1"}},
			true,
		},
		{
			Labels{{"alabel", "value1"}, {"blabel", "value2"}},
			Labels{{"blabel", "value2"}, {"alabel", "value1"}},
			true,
		},
	}

	for _, tc := range cases {
		assert.Equal(t, tc.wantEqual, tc.labels1.Equal(tc.labels2), "label1 %v, label2 %v", tc.labels1, tc.labels2)
		assert.Equal(t, tc.wantEqual, tc.labels2.Equal(tc.labels1), "label2 %v, label1 %v", tc.labels1, tc.labels2)
	}
}

func TestLabels_Add(t *testing.T) {
	cases := []struct {
		labels1    Labels
		labels2    Labels
		wantLabels Labels
	}{
		{nil, nil, nil},
		{
			nil,
			Labels{{"alabel", "value1"}},
			Labels{{"alabel", "value1"}},
		},
		{
			Labels{{"alabel", "value1"}},
			nil,
			Labels{{"alabel", "value1"}},
		},
		{
			Labels{{"alabel", "value1"}},
			Labels{{"blabel", "value1"}},
			Labels{{"alabel", "value1"}, {"blabel", "value1"}},
		},
		{
			Labels{{"alabel", "value1"}},
			Labels{{"alabel", "value2"}},
			Labels{{"alabel", "value1"}, {"alabel", "value2"}},
		},
		{
			Labels{{"alabel", "value1"}},
			Labels{{"alabel", "value1"}},
			Labels{{"alabel", "value1"}},
		},
		{
			Labels{{"alabel", "value1"}, {"blabel", "value2"}},
			Labels{{"blabel", "value2"}, {"alabel", "value1"}},
			Labels{{"alabel", "value1"}, {"blabel", "value2"}},
		},
	}

	for _, tc := range cases {
		assert.Equal(t, tc.wantLabels, tc.labels1.Add(tc.labels2), "label1 %v, label2 %v", tc.labels1, tc.labels2)
	}
}

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

func TestLabelsFromMap(t *testing.T) {
	cases := []struct {
		inMap      map[string]interface{}
		wantLabels Labels
	}{
		{
			nil,
			nil,
		},
		{
			map[string]interface{}{},
			nil,
		},
		{
			map[string]interface{}{"alabel": "value1"},
			Labels{{"alabel", "value1"}},
		},
	}

	for _, tc := range cases {
		assert.Equal(t, tc.wantLabels, LabelsFromMap(tc.inMap))
	}
}
