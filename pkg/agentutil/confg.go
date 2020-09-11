package agentutil

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/profefe/profefe/agent"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/version"
)

const (
	labelVersion = "version"
)

type Config struct {
	CollectorAddr string
	Service       string
	Labels        profile.Labels `json:",omitempty"`

	TickInterval time.Duration `json:",omitempty"`

	CPUProfile            time.Duration `json:",omitempty"`
	HeapProfile           bool          `json:",omitempty"`
	BlockProfile          bool          `json:",omitempty"`
	MutexProfile          bool          `json:",omitempty"`
	GoroutineProfile      bool          `json:",omitempty"`
	ThreadcreationProfile bool          `json:",omitempty"`
	//TraceProfile          bool          `json:",omitempty"`
}

func (conf *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&conf.CollectorAddr, "profefe.agent.collector-addr", "", "profefe collector public address to send profiling data")
	f.StringVar(&conf.Service, "profefe.agent.service-name", "profefe", "application service name")

	labels := (*labelsValue)(&conf.Labels)
	f.Var(labels, "profefe.agent.labels", "application labels")

	f.DurationVar(&conf.TickInterval, "profefe.agent.read-interval", 0, "how often agent reads profiling data")

	f.DurationVar(&conf.CPUProfile, "profefe.agent.enable-cpu-profile", 10*time.Second, "enables CPU profiling (sets profile collection duration)")
	f.BoolVar(&conf.HeapProfile, "profefe.agent.enable-heap-profile", false, "enables heap profiling")
	f.BoolVar(&conf.BlockProfile, "profefe.agent.enable-block-profile", false, "enables block (contention) profiling")
	f.BoolVar(&conf.MutexProfile, "profefe.agent.enable-mutex-profile", false, "enables mutex profiling")
	f.BoolVar(&conf.GoroutineProfile, "profefe.agent.enable-goroutine-profile", false, "enables goroutine profiling")
	f.BoolVar(&conf.ThreadcreationProfile, "profefe.agent.enable-thread-creation-profile", false, "enables thread creation profiling")
	//f.BoolVar(&conf.TraceProfile, "profefe.agent.enable-trace-profile", false, "enables trace profiling")
}

func (conf *Config) Start(ctx context.Context, logger *log.Logger) error {
	if conf.CollectorAddr == "" {
		logger.Infow("profefe-agent disabled: no collector address", "config", conf)
		return nil
	}

	opts := conf.options()

	opts = append(opts, agent.WithLogger(func(s string, v ...interface{}) {
		logger.Infow(fmt.Sprintf(s, v...))
	}))

	pffAgent, err := agent.Start(conf.CollectorAddr, conf.Service, opts...)
	if err != nil {
		return err
	}

	logger.Infow("profefe-agent is running", "config", conf)

	go func() {
		<-ctx.Done()
		pffAgent.Stop()
	}()

	return nil
}

func (conf *Config) options() (opts []agent.Option) {
	var labels []string
	for _, label := range conf.Labels {
		labels = append(labels, label.Key, label.Value)
	}

	labels = append(labels, labelVersion, version.Details().Version)

	opts = append(opts, agent.WithLabels(labels...))

	if conf.TickInterval != 0 {
		opts = append(opts, agent.WithTickInterval(conf.TickInterval))
	}

	if conf.CPUProfile != 0 {
		opts = append(opts, agent.WithCPUProfile(conf.CPUProfile))
	}
	if conf.HeapProfile {
		opts = append(opts, agent.WithHeapProfile())
	}
	if conf.BlockProfile {
		opts = append(opts, agent.WithBlockProfile())
	}
	if conf.MutexProfile {
		opts = append(opts, agent.WithMutexProfile())
	}
	if conf.GoroutineProfile {
		opts = append(opts, agent.WithGoroutineProfile())
	}
	if conf.ThreadcreationProfile {
		opts = append(opts, agent.WithThreadcreateProfile())
	}

	return opts
}

type labelsValue profile.Labels

func (v *labelsValue) Get() interface{} {
	return (*profile.Labels)(v)
}

func (v *labelsValue) String() string {
	return (*profile.Labels)(v).String()
}

func (v *labelsValue) Set(s string) error {
	var labels profile.Labels
	if err := labels.FromString(s); err != nil {
		return err
	}
	*v = labelsValue(labels)
	return nil
}
