package config

import (
	"flag"
	"time"

	"github.com/profefe/profefe/pkg/agentutil"
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
	AgentConfig agentutil.Config

	StorageType string
	Badger      BadgerConfig
	ClickHouse  ClickHouseConfig
	Kafka       KafkaConfig
	S3          S3Config
}

func (conf *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&conf.Addr, "addr", defaultAddr, "address to listen")
	f.DurationVar(&conf.ExitTimeout, "exit-timeout", defaultExitTimeout, "server shutdown timeout")

	conf.Logger.RegisterFlags(f)
	conf.AgentConfig.RegisterFlags(f)

	conf.registerStorageFlags(f)
}
