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
	"time"

	_ "github.com/lib/pq"
	"github.com/profefe/profefe/agent"
	"github.com/profefe/profefe/cmd/collector/api"
	"github.com/profefe/profefe/cmd/collector/middleware"
	"github.com/profefe/profefe/pkg/config"
	"github.com/profefe/profefe/pkg/filestore"
	"github.com/profefe/profefe/pkg/logger"
	"github.com/profefe/profefe/pkg/profile"
	pgstorage "github.com/profefe/profefe/pkg/storage/postgres"
	"github.com/profefe/profefe/version"
	"go.uber.org/zap"
)

const addr = ":10100"

const defaultDataRoot = "/tmp/profefe"

func main() {
	var conf config.Config
	conf.RegisterFlags(flag.CommandLine)

	flag.Parse()

	// TODO: init base logger
	baseLogger := zap.NewExample()
	defer baseLogger.Sync()

	log := logger.New(baseLogger)

	if err := run(context.Background(), log, conf); err != nil {
		log.Error(err)
	}
}

func run(ctx context.Context, log *logger.Logger, conf config.Config) error {
	var profileRepo *profile.Repository
	{
		fs, err := filestore.New(log, defaultDataRoot)
		if err != nil {
			return fmt.Errorf("could not create file storage: %v", err)
		}

		db, err := sql.Open("postgres", conf.Postgres.ConnString())
		if err != nil {
			return fmt.Errorf("could not connect to db: %v", err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			return fmt.Errorf("could not ping db: %v", err)
		}

		pgStorage, err := pgstorage.New(db, fs)
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
		Addr:    addr,
		Handler: handler,
	}

	errc := make(chan error, 1)
	go func() {
		log.Infow("server is running", "addr", addr)
		errc <- server.ListenAndServe()
	}()

	// start agent after server, because it sends to itself
	agentLogger := log.With("svc", "profefe")
	agent.Start(
		"profefe_collector",
		agent.WithCollector("http://"+addr),
		agent.WithCPUProfile(20*time.Second),
		agent.WithLabels("az", "home", "host", "localhost", "version", version.Version, "commit", version.Commit, "build", version.BuildTime),
		agent.WithLogger(func(format string, args ...interface{}) {
			agentLogger.Debugf(format, args...)
		}),
	)

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

	// must stop agent before server, because it sends to itself
	agent.Stop()

	return server.Shutdown(ctx)
}
