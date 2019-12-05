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

	"github.com/dgraph-io/badger"
	"github.com/profefe/profefe/pkg/config"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/middleware"
	"github.com/profefe/profefe/pkg/profefe"
	"github.com/profefe/profefe/pkg/storage"
	badgerStorage "github.com/profefe/profefe/pkg/storage/badger"
	"github.com/profefe/profefe/pkg/storage/s3"
	"github.com/profefe/profefe/version"
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

	if err := run(logger, conf); err != nil {
		logger.Error(err)
	}
}

func run(logger *log.Logger, conf config.Config) error {
	var (
		r storage.Reader
		w storage.Writer
	)
	if conf.Badger.Dir != "" {
		st, closer, err := initBadgerStorage(logger, conf)
		if err != nil {
			return err
		}
		defer closer.Close()
		r, w = st, st
	} else if conf.S3.Bucket != "" {
		st, err := initS3(logger, conf)
		if err != nil {
			return err
		}
		r, w = st, st
	} else {
		return fmt.Errorf("badger or s3 configuration required")
	}

	mux := http.NewServeMux()

	profefe.SetupRoutes(mux, logger, r, w)

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)

	h := middleware.LoggingHandler(os.Stdout, mux)
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

func initBadgerStorage(logger *log.Logger, conf config.Config) (*badgerStorage.Storage, io.Closer, error) {
	opt := badger.DefaultOptions(conf.Badger.Dir)
	db, err := badger.Open(opt)
	if err != nil {
		return nil, nil, xerrors.Errorf("could not open db: %w", err)
	}

	st := badgerStorage.New(logger, db, conf.Badger.ProfileTTL)
	return st, db, nil
}

func initS3(logger *log.Logger, conf config.Config) (*s3.Store, error) {
	return s3.NewStore(conf.S3.Region, conf.S3.Bucket)
}
