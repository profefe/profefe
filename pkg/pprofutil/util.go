package pprofutil

import pprofProfile "github.com/profefe/profefe/internal/pprof/profile"

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
