package main

import (
	"context"
	"expvar"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/profefe/profefe/pkg/config"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/middleware"
	"github.com/profefe/profefe/pkg/profefe"
	"github.com/profefe/profefe/pkg/storage"
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
		logger.Errorw(err.Error())
	}
}

func run(ctx context.Context, logger *log.Logger, conf config.Config, stdout io.Writer) error {
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	ctx, cancelTopMostCtx := context.WithCancel(ctx)
	defer cancelTopMostCtx()

	collector, querier, closer, err := initProfefe(logger, conf)
	if err != nil {
		return err
	}
	defer closer()

	mux := http.NewServeMux()

	profefe.SetupRoutes(mux, logger, prometheus.DefaultRegisterer, collector, querier)

	setupDebugRoutes(mux)

	// TODO(narqo) hardcoded stdout when setup request logging middleware
	h := middleware.LoggingHandler(stdout, mux)
	h = middleware.RecoveryHandler(logger, h)

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
		logger.Infow("exiting")
	case <-ctx.Done():
		logger.Infow("exiting", zap.Error(ctx.Err()))
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
	logger *log.Logger,
	conf config.Config,
) (
	collector *profefe.Collector,
	querier *profefe.Querier,
	closer func(),
	err error,
) {
	stypes, err := conf.StorageType()
	if err != nil {
		return nil, nil, nil, err
	}

	if len(stypes) > 1 {
		logger.Infow("WARNING: several storage types specified. Only first one is used for querying", "types", stypes, "quering type", stypes[0])
	}

	var (
		writers []storage.Writer
		reader  storage.Reader
		closers []io.Closer
	)

	assembleStorage := func(sw storage.Writer, sr storage.Reader, closer io.Closer) {
		writers = append(writers, sw)
		// only the first reader is used
		if reader == nil {
			reader = sr
		}
		if closer != nil {
			closers = append(closers, closer)
		}
	}

	initStorage := func(stype string) error {
		logger := logger.With(zap.String("storage", stype))

		switch stype {
		case config.StorageTypeBadger:
			st, closer, err := conf.Badger.CreateStorage(logger)
			if err == nil {
				assembleStorage(st, st, closer)
			}
			return err
		case config.StorageTypeS3:
			st, err := conf.S3.CreateStorage(logger)
			if err == nil {
				assembleStorage(st, st, nil)
			}
			return err
		case config.StorageTypeCH:
			st, closer, err := conf.ClickHouse.CreateStorage(logger)
			if err == nil {
				assembleStorage(st, st, closer)
			}
			return err
		case config.StorageTypeGCS:
			st, err := conf.GCS.CreateStorage(logger)
			if err == nil {
				assembleStorage(st, st, nil)
			}
			return err
		default:
			return fmt.Errorf("unknown storage type %q, config %v", stype, conf)
		}
	}

	for _, stype := range stypes {
		if err := initStorage(stype); err != nil {
			return nil, nil, nil, fmt.Errorf("could not init storage %q: %w", stype, err)
		}
	}

	closer = func() {
		for _, closer := range closers {
			if err := closer.Close(); err != nil {
				logger.Errorw("close closer", zap.Error(err))
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
