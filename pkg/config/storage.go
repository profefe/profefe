package config

import (
	"flag"
	"fmt"
	"strings"
	"time"
)

const (
	defaultStorageType = "auto"

	defaultBadgerRetentionPeriod = 5 * 24 * time.Hour
	defaultBadgerGCInternal      = 5 * time.Minute
	defaultBadgerGCDiscardRatio  = 0.7

	defaultS3Region     = "us-east-1"
	defaultS3MaxRetries = 3
)

func (conf *Config) registerStorageFlags(f *flag.FlagSet) {
	f.StringVar(&conf.StorageType, "storage-type", defaultStorageType, "storage type, comma-separated")

	conf.Badger.RegisterFlags(f)
	conf.ClickHouse.RegisterFlags(f)
	conf.Kafka.RegisterFlags(f)
	conf.S3.RegisterFlags(f)
}

func (conf *Config) StorageTypes() ([]string, error) {
	if conf.StorageType != "" && conf.StorageType != defaultStorageType {
		return strings.Split(conf.StorageType, ","), nil
	}

	// mimic the existing behaviour, where storage type is determined by storage-related flags
	if conf.Badger.Dir != "" {
		return []string{"badger"}, nil
	} else if conf.S3.Bucket != "" {
		return []string{"s3"}, nil
	} else {
		return nil, fmt.Errorf("storage configuration required")
	}
}

type BadgerConfig struct {
	Dir            string
	ProfileTTL     time.Duration
	GCInterval     time.Duration
	GCDiscardRatio float64
}

func (conf *BadgerConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&conf.Dir, "badger.dir", "", "badger data dir")
	f.DurationVar(&conf.ProfileTTL, "badger.profile-ttl", defaultBadgerRetentionPeriod, "badger profile data ttl")
	f.DurationVar(&conf.GCInterval, "badger.gc-interval", defaultBadgerGCInternal, "interval in which the badger garbage collector is run")
	f.Float64Var(&conf.GCDiscardRatio, "badger.gc-discard-ratio", defaultBadgerGCDiscardRatio, "a badger file is rewritten if this ratio of the file can be discarded")
}

type S3Config struct {
	EndpointURL string
	DisableSSL  bool
	Region      string
	Bucket      string
	MaxRetries  int
}

func (conf *S3Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&conf.EndpointURL, "s3.endpoint-url", "", "override default URL to s3 service")
	f.BoolVar(&conf.DisableSSL, "s3.disable-ssl", false, "disable SSL verification")
	f.StringVar(&conf.Region, "s3.region", defaultS3Region, "object storage region")
	f.StringVar(&conf.Bucket, "s3.bucket", "", "s3 bucket profile destination")
	f.IntVar(&conf.MaxRetries, "s3.max-retries", defaultS3MaxRetries, "s3 request maximum number of retries")
}

type KafkaConfig struct {
	Brokers string
	Topic   string
}

func (conf *KafkaConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&conf.Brokers, "kafka.brokers", "", "kafka bootstrap brokers")
	f.StringVar(&conf.Topic, "kafka.topic", "", "kafka topic")
}

type ClickHouseConfig struct {
	DSN          string
	DropDatabase bool
}

func (config *ClickHouseConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&config.DSN, "clickhouse.dsn", "", "clickhouse dsn")
	f.BoolVar(&config.DropDatabase, "clickhouse.dropdb", false, "drop database on start")
}
