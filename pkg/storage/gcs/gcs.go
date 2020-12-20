package gcs

import (
	"context"
	"flag"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/profefe/profefe/pkg/log"
)

const (
	defaultMaxRetries = 3
)

type Config struct {
	Bucket      string
	MaxRetries  int
}

func (conf *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&conf.Bucket, "gcs.bucket", "", "gcs bucket profile destination")
	f.IntVar(&conf.MaxRetries, "gcs.max-retries", defaultMaxRetries, "gcs request maximum number of retries")
}

func (conf *Config) CreateStorage(logger *log.Logger) (*Storage, error) {
	client, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, fmt.Errorf("could not create gcs client: %w", err)
	}
	return NewStorage(logger, client, conf.Bucket), nil
}
