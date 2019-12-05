package config

import (
	"flag"
	"time"

	"github.com/profefe/profefe/pkg/log"
)

const (
	defaultAddr        = ":10100"
	defaultExitTimeout = 5 * time.Second

	defaultRetentionPeriod = 5 * 24 * time.Hour
	defaultGCInternal      = 5 * time.Minute
	defaultGCDiscardRatio  = 0.7
)

type Config struct {
	Addr        string
	ExitTimeout time.Duration
	Logger      log.Config
	Badger      BadgerConfig
	S3          S3Config
}

func (conf *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&conf.Addr, "addr", defaultAddr, "address to listen")
	f.DurationVar(&conf.ExitTimeout, "exit-timeout", defaultExitTimeout, "server shutdown timeout")

	conf.Logger.RegisterFlags(f)
	conf.Badger.RegisterFlags(f)
	conf.S3.RegisterFlags(f)
}

type BadgerConfig struct {
	Dir            string
	ProfileTTL     time.Duration
	GCInterval     time.Duration
	GCDiscardRatio float64
}

func (conf *BadgerConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&conf.Dir, "badger.dir", "", "badger data dir")
	f.DurationVar(&conf.ProfileTTL, "badger.profile-ttl", defaultRetentionPeriod, "badger profile data ttl")
	f.DurationVar(&conf.GCInterval, "badger.gc-interval", defaultGCInternal, "interval in which the badger garbage collector is run")
	f.Float64Var(&conf.GCDiscardRatio, "badger.gc-discard-ratio", defaultGCDiscardRatio, "a badger file is rewritten if this ratio of the file can be discarded")
}

type S3Config struct {
	Region string
	Bucket string
}

func (conf *S3Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&conf.Region, "s3.region", "us-east-1", "AWS region")
	f.StringVar(&conf.Bucket, "s3.bucket", "", "s3 bucket profile destination")
}
