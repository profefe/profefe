package config

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/profefe/profefe/pkg/agentutil"
	"github.com/profefe/profefe/pkg/log"
	storageBadger "github.com/profefe/profefe/pkg/storage/badger"
	storageCH "github.com/profefe/profefe/pkg/storage/clickhouse"
	storageS3 "github.com/profefe/profefe/pkg/storage/s3"
)

const (
	defaultAddr        = ":10100"
	defaultExitTimeout = 5 * time.Second

	defaultStorageType = "auto"
	StorageTypeBadger  = "badger"
	StorageTypeS3      = "s3"
	StorageTypeCH      = "clickhouse"
)

var storageTypes = []string{StorageTypeBadger, StorageTypeCH, StorageTypeS3}

type Config struct {
	Addr        string
	ExitTimeout time.Duration
	Logger      log.Config
	AgentConfig agentutil.Config

	storageType string
	Badger      storageBadger.Config
	ClickHouse  storageCH.Config
	S3          storageS3.Config
}

func (conf *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&conf.Addr, "addr", defaultAddr, "address to listen")
	f.DurationVar(&conf.ExitTimeout, "exit-timeout", defaultExitTimeout, "server shutdown timeout")

	conf.Logger.RegisterFlags(f)
	conf.AgentConfig.RegisterFlags(f)

	f.StringVar(&conf.storageType, "storage-type", defaultStorageType, fmt.Sprintf("storage type: %s", strings.Join(storageTypes, ", ")))

	conf.Badger.RegisterFlags(f)
	conf.ClickHouse.RegisterFlags(f)
	conf.S3.RegisterFlags(f)
}

func (conf *Config) StorageType() ([]string, error) {
	if conf.storageType != "" && conf.storageType != defaultStorageType {
		return strings.Split(conf.storageType, ","), nil
	}

	// mimic previous behaviour, where storage type was determined by storage-related flags
	if conf.Badger.Dir != "" {
		return []string{StorageTypeBadger}, nil
	} else if conf.S3.Bucket != "" {
		return []string{StorageTypeS3}, nil
	}
	return nil, fmt.Errorf("storage configuration required")
}
