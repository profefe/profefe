package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/profefe/profefe/pkg/config"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/middleware"
	"github.com/profefe/profefe/pkg/profefe"
	storageBadger "github.com/profefe/profefe/pkg/storage/badger"
	"github.com/profefe/profefe/version"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
)

func main() {
	printVersion := flag.Bool("version", false, "print version and exit")

	var conf config.Config
	conf.RegisterFlags(flag.CommandLine)

	flag.Parse()

	if *printVersion {
		fmt.Println(version.String())
		os.Exit(1)
	}

	logger, err := conf.Logger.Build()
	if err != nil {
		panic(err)
	}

	if err := run(logger, conf, os.Stdout); err != nil {
		logger.Error(err)
	}
}

func run(logger *log.Logger, conf config.Config, stdout io.Writer) error {
	st, closer, err := initBadgerStorage(logger, conf)
	if err != nil {
		return err
	}
	defer closer.Close()

	mux := http.NewServeMux()

	profefe.SetupRoutes(mux, logger, prometheus.DefaultRegisterer, st, st)

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

	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	select {
	case <-sigs:
		logger.Info("exiting")
	case err := <-errc:
		if err != http.ErrServerClosed {
			return xerrors.Errorf("terminated: %w", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), conf.ExitTimeout)
	defer cancel()

	return server.Shutdown(ctx)
}

func initBadgerStorage(logger *log.Logger, conf config.Config) (*storageBadger.Storage, io.Closer, error) {
	opt := badger.DefaultOptions(conf.Badger.Dir)
	db, err := badger.Open(opt)
	if err != nil {
		return nil, nil, xerrors.Errorf("could not open db: %w", err)
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
				logger.Errorw("badger failed to run value log garbage collection", zap.Error(err))
			}
			time.Sleep(conf.Badger.GCInterval)
		}
	}()

	st := storageBadger.New(logger, db, conf.Badger.ProfileTTL)
	return st, db, nil
}

func setupDebugRoutes(mux *http.ServeMux) {
	// pprof handlers, see https://github.com/golang/go/blob/master/src/net/http/pprof/pprof.go
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))

	// prometheus handlers
	mux.Handle("/debug/metrics", promhttp.Handler())
}
