package clickhouse

import (
	"database/sql"
	"flag"
	"io"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/profefe/profefe/pkg/log"
)

type Config struct {
	DSN               string
	DropDatabase      bool
	SamplesWriterPool int
}

func (conf *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&conf.DSN, "clickhouse.dsn", "", "clickhouse dsn")
	f.BoolVar(&conf.DropDatabase, "clickhouse.dropdb", false, "drop database on start")
	f.IntVar(&conf.SamplesWriterPool, "clickhouse.samples-writer.pool-size", 0, "samples writer workers pool size (zero means don't use pool)")
}

func (conf *Config) CreateStorage(logger *log.Logger) (*Storage, io.Closer, error) {
	if err := SetupDB(logger, conf.DSN, conf.DropDatabase); err != nil {
		return nil, nil, err
	}

	db, err := sql.Open("clickhouse", conf.DSN)
	if err != nil {
		return nil, nil, err
	}

	if err := SetupTables(db); err != nil {
		return nil, nil, err
	}

	closers := multiCloser{db}

	profilesWriter := NewProfilesWriter(logger, db)
	samplesWriter := NewSamplesWriter(logger, db)

	if conf.SamplesWriterPool > 0 {
		writer := withPool(conf.SamplesWriterPool, logger, samplesWriter)
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
