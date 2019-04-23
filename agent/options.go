package agent

import (
	"net/http"
	"time"
)

type Option func(a *agent)

func WithCPUProfile(duration time.Duration) Option {
	return func(a *agent) {
		a.CPUProfile = true
		a.ProfileDuration = duration
	}
}

func WithHeapProfile() Option {
	return func(a *agent) {
		a.HeapProfile = true
	}
}

// TODO(narqo): support the rest of profile types
//func WithBlockProfile() Option {
//	return func(a *agent) {
//		a.BlockProfile = true
//	}
//}
//
//func WithMuxProfile() Option {
//	return func(a *agent) {
//		a.MuxProfile = true
//	}
//}

func WithCollector(addr string) Option {
	return func(a *agent) {
		a.collectorAddr = addr
	}
}

func WithLabels(args ...string) Option {
	if len(args)%2 != 0 {
		panic("agent.WithLabels: uneven number of arguments, expected key-value pairs")
	}
	return func(a *agent) {
		for i := 0; i+1 < len(args); i += 2 {
			a.labels[args[i]] = args[i+1]
		}
	}
}

func WithHTTPClient(c *http.Client) Option {
	return func(a *agent) {
		a.rawClient = c
	}
}

func WithLogger(logf func(string, ...interface{})) Option {
	return func(a *agent) {
		a.logf = logf
	}
}
