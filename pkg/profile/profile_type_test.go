package profile

import "testing"

func TestProfileTypeFromString(t *testing.T) {
	cases := []struct {
		in   string
		want ProfileType
	}{
		{"cpu", CPUProfile},
		{"blah", UnknownProfile},
	}

	for _, tc := range cases {
		if got := ProfileTypeFromString(tc.in); tc.want != got {
			t.Errorf("ProfileTypeFromString(%q): got %v, want %v", tc.in, got, tc.want)
		}
	}
}
