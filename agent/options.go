package agent

import (
	"net/http"
	"net/url"
	"time"
)

type Option func(a *Agent)

func WithCPUProfile(duration time.Duration) Option {
	return func(a *Agent) {
		a.CPUProfile = true
		a.CPUProfileDuration = duration
	}
}

func WithHeapProfile() Option {
	return func(a *Agent) {
		a.HeapProfile = true
	}
}

func WithBlockProfile() Option {
	return func(a *Agent) {
		a.BlockProfile = true
	}
}

func WithMutexProfile() Option {
	return func(a *Agent) {
		a.MutexProfile = true
	}
}

func WithGoroutineProfile() Option {
	return func(a *Agent) {
		a.GoroutineProfile = true
	}
}

func WithThreadcreateProfile() Option {
	return func(a *Agent) {
		a.ThreadcreateProfile = true
	}
}

func WithLabels(args ...string) Option {
	if len(args)%2 != 0 {
		panic("agent.WithLabels: uneven number of arguments, expected key-value pairs")
	}
	return func(a *Agent) {
		first := true
		for i := 0; i+1 < len(args); i += 2 {
			if !first {
				a.rawLabels.WriteByte(',')
			}
			first = false
			k, v := args[i], args[i+1]
			a.rawLabels.WriteString(url.QueryEscape(k))
			a.rawLabels.WriteByte('=')
			a.rawLabels.WriteString(url.QueryEscape(v))
		}
	}
}

func WithHTTPClient(c *http.Client) Option {
	return func(a *Agent) {
		a.rawClient = c
	}
}

func WithLogger(logf func(string, ...interface{})) Option {
	return func(a *Agent) {
		a.logf = logf
	}
}
