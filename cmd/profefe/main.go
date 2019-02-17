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

	"github.com/profefe/profefe/cmd/profefe/api"
	"github.com/profefe/profefe/cmd/profefe/middleware"
	"github.com/profefe/profefe/pkg/config"
	"github.com/profefe/profefe/pkg/logger"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/version"
	"go.uber.org/zap"

	_ "github.com/lib/pq"
	pgstorage "github.com/profefe/profefe/pkg/storage/postgres"
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

	// TODO: init base logger
	baseLogger, _ := zap.NewDevelopment()
	defer baseLogger.Sync()

	log := logger.New(baseLogger)

	if err := run(context.Background(), log, conf); err != nil {
		log.Error(err)
	}
}

func run(ctx context.Context, log *logger.Logger, conf config.Config) error {
	var profileRepo *profile.Repository
	{
		db, err := sql.Open("postgres", conf.Postgres.ConnString())
		if err != nil {
			return fmt.Errorf("could not connect to db: %v", err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			return fmt.Errorf("could not ping db: %v", err)
		}

		pgStorage, err := pgstorage.New(log.With("svc", "pg"), db)
		if err != nil {
			return fmt.Errorf("could not create new pg storage: %v", err)
		}

		profileRepo = profile.NewRepository(log, pgStorage)
	}

	mux := http.NewServeMux()
	apiHandler := api.NewAPIHandler(log, profileRepo)
	apiHandler.RegisterRoutes(mux)

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)

	handler := middleware.LoggingHandler(os.Stdout, mux)
	handler = middleware.RecoveryHandler(handler)

	server := http.Server{
		Addr:    conf.Addr,
		Handler: handler,
	}

	errc := make(chan error, 1)
	go func() {
		log.Infow("server is running", "addr", server.Addr)
		errc <- server.ListenAndServe()
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	select {
	case <-sigs:
		log.Info("exiting")
	case err := <-errc:
		if err != http.ErrServerClosed {
			return fmt.Errorf("terminated: %v", err)
		}
	}

	return server.Shutdown(ctx)
}
