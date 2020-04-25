package badger

import (
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/profefe/profefe/pkg/log"
	"go.uber.org/zap"
)

const (
	defaultRetentionPeriod = 5 * 24 * time.Hour
	defaultGCInternal      = 5 * time.Minute
	defaultGCDiscardRatio  = 0.7
)

type Config struct {
	Dir            string
	ProfileTTL     time.Duration
	GCInterval     time.Duration
	GCDiscardRatio float64
}

func (conf *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&conf.Dir, "badger.dir", "", "badger data dir")
	f.DurationVar(&conf.ProfileTTL, "badger.data-ttl", defaultRetentionPeriod, "badger data ttl")
	f.DurationVar(&conf.GCInterval, "badger.gc-interval", defaultGCInternal, "interval in which the badger garbage collector is run")
	f.Float64Var(&conf.GCDiscardRatio, "badger.gc-discard-ratio", defaultGCDiscardRatio, "a badger file is rewritten if this ratio of the file can be discarded")
}

func (conf *Config) CreateStorage(logger *log.Logger) (*Storage, io.Closer, error) {
	opt := badger.DefaultOptions(conf.Dir)
	db, err := badger.Open(opt)
	if err != nil {
		return nil, nil, fmt.Errorf("could not open db: %w", err)
	}

	// run values garbage collection, see https://github.com/dgraph-io/badger#garbage-collection
	go func() {
		for {
			err := db.RunValueLogGC(conf.GCDiscardRatio)
			if err == nil {
				// nil error is not the expected behaviour, because
				// badger returns ErrNoRewrite as an indicator that everything went ok
				continue
			} else if err != badger.ErrNoRewrite {
				logger.Errorw("failed to run value log garbage collection", zap.Error(err))
			}
			time.Sleep(conf.GCInterval)
		}
	}()

	return NewStorage(logger, db, conf.ProfileTTL), db, nil
}
