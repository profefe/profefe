package profile

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJoinIDs(t *testing.T) {
	cases := []struct {
		ids     []ID
		want    string
		wantErr bool
	}{
		{
			nil,
			"",
			false,
		},
		{
			[]ID{},
			"",
			false,
		},
		{
			[]ID{TestID},
			string(TestID),
			false,
		},
		{
			[]ID{"bpc00mript33iv4net01", "bpc00mript33iv4net02", "bpc00mript33iv4net03"},
			"bpc00mript33iv4net01+bpc00mript33iv4net02+bpc00mript33iv4net03",
			false,
		},
		{
			[]ID{"P0.svc1/1/bpc00mript33iv4net01,k1=v1,k2=v2", "P0.svc1/1/bpc00mript33iv4net02,k1=v1,k2=v2"},
			"P0.svc1/1/bpc00mript33iv4net01,k1=v1,k2=v2+P0.svc1/1/bpc00mript33iv4net02,k1=v1,k2=v2",
			false,
		},
		{
			[]ID{"P0.svc1/1/bpc00mript33iv4net01+k1=v1,k2=v2"},
			"",
			true,
		},
	}

	for n, tc := range cases {
		t.Run(fmt.Sprintf("case=%d", n), func(t *testing.T) {
			s, err := JoinIDs(tc.ids...)
			require.True(t, (err != nil) == tc.wantErr, "want err %v, got %v", tc.wantErr, err)
			require.Equal(t, tc.want, s)
		})
	}
}

func TestSplitIDs(t *testing.T) {
	cases := []struct {
		ids     string
		want    []ID
		wantErr bool
	}{
		{
			"",
			nil,
			false,
		},
		{
			string(TestID),
			[]ID{TestID},
			false,
		},
		{
			"bpc00mript33iv4net01+bpc00mript33iv4net02+bpc00mript33iv4net03",
			[]ID{"bpc00mript33iv4net01", "bpc00mript33iv4net02", "bpc00mript33iv4net03"},
			false,
		},
		{
			"P0.svc1/1/bpc00mript33iv4net01,k1=v1,k2=v2+P0.svc1/1/bpc00mript33iv4net02,k1=v1,k2=v2",
			[]ID{"P0.svc1/1/bpc00mript33iv4net01,k1=v1,k2=v2", "P0.svc1/1/bpc00mript33iv4net02,k1=v1,k2=v2"},
			false,
		},
		{
			"bpc00mript33iv4net01++bpc00mript33iv4net02",
			nil,
			true,
		},
	}

	for n, tc := range cases {
		t.Run(fmt.Sprintf("case=%d", n), func(t *testing.T) {
			ids, err := SplitIDs(tc.ids)
			require.True(t, (err != nil) == tc.wantErr, "want err %v, got %v", tc.wantErr, err)
			require.ElementsMatch(t, tc.want, ids)
		})
	}
}
