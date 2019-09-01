package config

import (
	"flag"
	"time"

	"github.com/profefe/profefe/pkg/log"
)

const (
	defaultAddr        = ":10100"
	defaultExitTimeout = 5 * time.Second
)

type Config struct {
	Addr        string
	ExitTimeout time.Duration
	Logger      log.Config
	Badger      BadgerConfig
}

func (conf *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&conf.Addr, "addr", defaultAddr, "address to listen")
	f.DurationVar(&conf.ExitTimeout, "exit-timeout", defaultExitTimeout, "server shutdown timeout")

	conf.Logger.RegisterFlags(f)
	conf.Badger.RegisterFlags(f)
}

type BadgerConfig struct {
	Dir        string
	ProfileTTL time.Duration
}

func (conf *BadgerConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&conf.Dir, "badger.dir", "data", "badger data dir")
	f.DurationVar(&conf.ProfileTTL, "badger.profile-ttl", 3*24*time.Hour, "badger profile data ttl")
}
