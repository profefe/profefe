package pprofutil

import (
	"bytes"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
)

// SampleAddLabel adds a key-value pair to the sample.
// Note that, non-empty valueStr take precedence over valueNum.
func SampleAddLabel(s *pprofProfile.Sample, key, valueStr string, valueNum int64) {
	if valueStr != "" {
		if s.Label == nil {
			s.Label = make(map[string][]string)
		}
		s.Label[key] = append(s.Label[key], valueStr)
	} else {
		if s.NumLabel == nil {
			s.NumLabel = make(map[string][]int64)
		}
		s.NumLabel[key] = append(s.NumLabel[key], valueNum)
	}
}

// ProfilesEqual is a test helper that checks whether two profiles equal.
func ProfilesEqual(pp1, pp2 *pprofProfile.Profile) bool {
	s1 := getProfileString(pp1)
	s2 := getProfileString(pp2)
	return s1 == s2
}

// returns a simplified string representation of a profile.
func getProfileString(pp *pprofProfile.Profile) string {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		panic(err)
	}
	defer os.Remove(f.Name())

	pp.Compact().WriteUncompressed(f)

	var buf bytes.Buffer
	cmd := exec.Command("go", "tool", "pprof", "-top", f.Name())
	cmd.Stdout = &buf
	if err := cmd.Run(); err != nil {
		panic(err)
	}

	s := buf.String()
	// strip profile header
	if n := strings.Index(s, "Showing nodes"); n > 0 {
		return s[n:]
	}
	return s
}
