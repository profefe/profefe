package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"
	"github.com/profefe/profefe/pkg/config"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/middleware"
	"github.com/profefe/profefe/pkg/profefe"
	"github.com/profefe/profefe/pkg/storage"
	pgstorage "github.com/profefe/profefe/pkg/storage/postgres"
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

	if err := run(context.Background(), logger, conf); err != nil {
		logger.Error(err)
	}
}

func run(ctx context.Context, logger *log.Logger, conf config.Config) error {
	var st storage.Storage
	{
		db, err := sql.Open("postgres", conf.Postgres.ConnString())
		if err != nil {
			return xerrors.Errorf("could not connect to db: %w", err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			return xerrors.Errorf("could not ping db: %w", err)
		}

		st, err = pgstorage.New(logger.With("storage", "pg"), db)
		if err != nil {
			return xerrors.Errorf("could not create new pg storage: %w", err)
		}
	}

	mux := http.NewServeMux()

	mux.Handle("/api/0/profile", profefe.NewProfileHandler(logger, st))
	mux.HandleFunc("/api/0/version", profefe.VersionHandler)

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

	ctx, cancel := context.WithTimeout(ctx, conf.ExitTimeout)
	defer cancel()

	return server.Shutdown(ctx)
}
