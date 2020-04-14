package main

import (
	"context"
	"database/sql"
	"expvar"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/Shopify/sarama"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/dgraph-io/badger"
	"github.com/profefe/profefe/pkg/config"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/middleware"
	"github.com/profefe/profefe/pkg/profefe"
	"github.com/profefe/profefe/pkg/storage"
	storageBadger "github.com/profefe/profefe/pkg/storage/badger"
	storageCH "github.com/profefe/profefe/pkg/storage/clickhouse"
	storageKafka "github.com/profefe/profefe/pkg/storage/kafka"
	storageS3 "github.com/profefe/profefe/pkg/storage/s3"
	"github.com/profefe/profefe/version"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func main() {
	printVersion := flag.Bool("version", false, "print version and exit")

	var conf config.Config
	conf.RegisterFlags(flag.CommandLine)

	flag.Parse()

	if *printVersion {
		fmt.Println(version.Details())
		os.Exit(1)
	}

	logger, err := conf.Logger.Build()
	if err != nil {
		panic(err)
	}

	if err := run(context.Background(), logger, conf, os.Stdout); err != nil {
		logger.Error(err)
	}
}

func run(ctx context.Context, logger *log.Logger, conf config.Config, stdout io.Writer) error {
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	ctx, cancelTopMostCtx := context.WithCancel(ctx)
	defer cancelTopMostCtx()

	collector, querier, closer, err := initProfefe(ctx, logger, conf)
	if err != nil {
		return err
	}
	defer closer()

	mux := http.NewServeMux()

	profefe.SetupRoutes(mux, logger, prometheus.DefaultRegisterer, collector, querier)

	setupDebugRoutes(mux)

	// TODO(narqo) hardcoded stdout when setup logging middleware
	h := middleware.LoggingHandler(stdout, mux)
	h = middleware.RecoveryHandler(h)

	server := http.Server{
		Addr:    conf.Addr,
		Handler: h,
	}

	errc := make(chan error, 1)
	go func() {
		logger.Infow("server is running", "addr", server.Addr)
		errc <- server.ListenAndServe()
	}()

	if err := setupProfefeAgent(ctx, logger, conf); err != nil {
		return err
	}

	select {
	case <-sigs:
		logger.Info("exiting")
	case <-ctx.Done():
		logger.Info("exiting", zap.Error(ctx.Err()))
	case err := <-errc:
		if err != http.ErrServerClosed {
			return fmt.Errorf("terminated: %w", err)
		}
	}

	cancelTopMostCtx()

	// create new context because top-most is already canceled
	ctx, cancel := context.WithTimeout(context.Background(), conf.ExitTimeout)
	defer cancel()

	return server.Shutdown(ctx)
}

func initProfefe(
	ctx context.Context,
	logger *log.Logger,
	conf config.Config,
) (
	collector *profefe.Collector,
	querier *profefe.Querier,
	closer func(),
	err error,
) {
	stypes, err := conf.StorageTypes()
	if err != nil {
		return nil, nil, nil, err
	}

	if len(stypes) > 1 {
		logger.Infof("WARNING: several storage types specified: %s. Only first one %q is used for querying", stypes, stypes[0])
	}

	var (
		writers []storage.Writer
		reader  storage.Reader
		closers []io.Closer
	)

	appendStorage := func(sw storage.Writer, sr storage.Reader, closer io.Closer) {
		writers = append(writers, sw)
		// XXX(narqo) only the first storage type is used for querying
		if reader == nil {
			reader = sr
		}
		if closer != nil {
			closers = append(closers, closer)
		}
	}

	initStorage := func(stype string) error {
		switch stype {
		case "badger":
			st, closer, err := initBadgerStorage(logger, conf)
			if err != nil {
				return err
			}
			appendStorage(st, st, closer)
		case "s3":
			st, err := initS3Storage(logger, conf)
			if err != nil {
				return err
			}
			appendStorage(st, st, nil)
		case "kafka":
			sw, closer, err := initKafkaWriter(logger, conf)
			if err != nil {
				return err
			}
			appendStorage(sw, nil, closer)
		case "clickhouse":
			st, closer, err := initClickHouseStorage(ctx, logger, conf)
			if err != nil {
				return err
			}
			appendStorage(st, nil, closer)
		default:
			return fmt.Errorf("unknown storage type %q, config %v", stype, conf)
		}
		return nil
	}

	for _, stype := range stypes {
		if err := initStorage(stype); err != nil {
			return nil, nil, nil, fmt.Errorf("could not init storage %q: %w", stype, err)
		}
	}

	closer = func() {
		for _, closer := range closers {
			if err := closer.Close(); err != nil {
				logger.Error(err)
			}
		}
	}

	var writer storage.Writer
	if len(writers) == 1 {
		writer = writers[0]
	} else {
		writer = storage.NewMultiWriter(writers...)
	}
	return profefe.NewCollector(logger, writer), profefe.NewQuerier(logger, reader), closer, nil
}

func initBadgerStorage(logger *log.Logger, conf config.Config) (*storageBadger.Storage, io.Closer, error) {
	logger = logger.With(zap.String("storage", "badger"))

	opt := badger.DefaultOptions(conf.Badger.Dir)
	db, err := badger.Open(opt)
	if err != nil {
		return nil, nil, fmt.Errorf("could not open db: %w", err)
	}

	// run values garbage collection, see https://github.com/dgraph-io/badger#garbage-collection
	go func() {
		for {
			err := db.RunValueLogGC(conf.Badger.GCDiscardRatio)
			if err == nil {
				// nil error is not the expected behaviour, because
				// badger returns ErrNoRewrite as an indicator that everything went ok
				continue
			} else if err != badger.ErrNoRewrite {
				logger.Errorw("failed to run value log garbage collection", zap.Error(err))
			}
			time.Sleep(conf.Badger.GCInterval)
		}
	}()

	st := storageBadger.New(logger, db, conf.Badger.ProfileTTL)
	return st, db, nil
}

func initS3Storage(logger *log.Logger, conf config.Config) (*storageS3.Storage, error) {
	logger = logger.With(zap.String("storage", "s3"))

	var forcePathStyle bool
	if conf.S3.EndpointURL != "" {
		// should one use custom object storage service (e.g. Minio), path-style addressing needs to be set
		forcePathStyle = true
	}
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(conf.S3.EndpointURL),
		DisableSSL:       aws.Bool(conf.S3.DisableSSL),
		Region:           aws.String(conf.S3.Region),
		MaxRetries:       aws.Int(conf.S3.MaxRetries),
		S3ForcePathStyle: aws.Bool(forcePathStyle),
	})
	if err != nil {
		return nil, fmt.Errorf("could not create s3 session: %w", err)
	}
	return storageS3.New(logger, s3.New(sess), conf.S3.Bucket), nil
}

func initKafkaWriter(logger *log.Logger, conf config.Config) (*storageKafka.Writer, io.Closer, error) {
	logger = logger.With(zap.String("storage", "kafka"))

	producerConf := sarama.NewConfig()
	brokers := strings.Split(conf.Kafka.Brokers, ",")

	producer, err := sarama.NewAsyncProducer(brokers, producerConf)
	if err != nil {
		return nil, nil, err
	}

	return storageKafka.New(logger, producer, conf.Kafka.Topic), producer, nil
}

func initClickHouseStorage(ctx context.Context, logger *log.Logger, conf config.Config) (*storageCH.Storage, io.Closer, error) {
	logger = logger.With(zap.String("storage", "clickhouse"))

	dsnURL, err := url.Parse(conf.ClickHouse.DSN)
	if err != nil {
		return nil, nil, err
	}
	database := dsnURL.Query().Get("database")
	if database == "" {
		return nil, nil, fmt.Errorf("clickhouse: no database in DNS %q", conf.ClickHouse.DSN)
	}

	q := dsnURL.Query()
	q.Del("database")
	dsn := dsnURL
	dsn.RawQuery = q.Encode()

	initDB, err := sql.Open("clickhouse", dsn.String())
	if err != nil {
		return nil, nil, fmt.Errorf("clickhouse: failed to open init db: %w", err)
	}
	defer initDB.Close()

	if conf.ClickHouse.DropDatabase {
		logger.Infof("drop database %q", database)
		if _, err := initDB.ExecContext(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %s`, database)); err != nil {
			return nil, nil, err
		}
	}

	if _, err := initDB.ExecContext(ctx, fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, database)); err != nil {
		return nil, nil, err
	}

	db, err := sql.Open("clickhouse", conf.ClickHouse.DSN)
	if err != nil {
		return nil, nil, err
	}

	st, err := storageCH.New(logger, db, database)
	if err != nil {
		return nil, nil, err
	}
	return st, db, nil
}

func setupDebugRoutes(mux *http.ServeMux) {
	// pprof handlers, see https://github.com/golang/go/blob/release-branch.go1.13/src/net/http/pprof/pprof.go
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))

	// expvar handlers, see https://github.com/golang/go/blob/release-branch.go1.13/src/expvar/expvar.go
	mux.Handle("/debug/vars", expvar.Handler())

	// prometheus handlers
	mux.Handle("/debug/metrics", promhttp.Handler())
}

func setupProfefeAgent(ctx context.Context, logger *log.Logger, conf config.Config) error {
	logger = logger.With(zap.String("component", "profefe-agent"))
	return conf.AgentConfig.Start(ctx, logger)
}
