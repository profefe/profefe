package clickhouse

import (
	"database/sql"
	"flag"
	"fmt"
	"io"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/profefe/profefe/pkg/log"
)

type Config struct {
	DSN string

	SamplesWriterPoolSize int
}

func (conf *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&conf.DSN, "clickhouse.dsn", "", "clickhouse dsn")
	f.IntVar(&conf.SamplesWriterPoolSize, "clickhouse.samples-writer.pool-size", 0, "samples writer workers pool size (zero means don't use pool)")
}

func (conf *Config) CreateStorage(logger *log.Logger) (*Storage, io.Closer, error) {
	db, err := sql.Open("clickhouse", conf.DSN)
	if err != nil {
		return nil, nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, nil, fmt.Errorf("failed to ping db: %w", err)
	}

	closers := multiCloser{db}

	profilesWriter := NewProfilesWriter(logger, db)
	samplesWriter := NewSamplesWriter(logger, db)

	if conf.SamplesWriterPoolSize > 0 {
		writer := withPool(conf.SamplesWriterPoolSize, logger, samplesWriter)
		closers = append(closers, writer)
		samplesWriter = writer
	}

	st, err := NewStorage(logger, db, profilesWriter, samplesWriter)
	if err != nil {
		return nil, nil, err
	}
	return st, closers, nil
}

type multiCloser []io.Closer

func (c multiCloser) Close() error {
	for _, closer := range c {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	return nil
}
