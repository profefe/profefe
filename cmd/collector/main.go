package main

import (
	"context"
	"database/sql"
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
	"github.com/profefe/profefe/pkg/filestore"
	"github.com/profefe/profefe/pkg/logger"
	"github.com/profefe/profefe/pkg/profile"
	pqstorage "github.com/profefe/profefe/pkg/storage/postgres"
	"github.com/profefe/profefe/version"
	"go.uber.org/zap"
)

const addr = ":10100"

const (
	defaultDataRoot = "/tmp/profefe"

	postgresUser     = "postgres"
	postgresPassword = "postgres"
	postgresHost     = "127.0.0.1"
	postgresDB       = "profiles"
)

func main() {
	ctx := context.Background()

	// TODO: init base logger
	baseLogger := zap.NewExample()
	defer baseLogger.Sync()

	log := logger.New(baseLogger)

	agentLogger := log.With("svc", "agent")
	agent.Start(
		"profefe_collector",
		agent.WithCollector("http://"+addr),
		agent.WithCPUProfile(20*time.Second),
		agent.WithLabels("az", "home", "host", "localhost", "version", version.Version, "commit", version.Commit, "build", version.BuildTime),
		agent.WithLogger(func(format string, args ...interface{}) {
			agentLogger.Debugf("profefe: %s", fmt.Sprintf(format, args...))
		}),
	)

	if err := run(ctx, log); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, log *logger.Logger) error {
	var profileRepo *profile.Repository
	{
		fs, err := filestore.New(log, defaultDataRoot)
		if err != nil {
			return fmt.Errorf("could not create file storage: %v", err)
		}

		dbURL := fmt.Sprintf(
			"postgres://%s:%s@%s/%s?sslmode=disable",
			postgresUser,
			postgresPassword,
			postgresHost,
			postgresDB,
		)
		db, err := sql.Open("postgres", dbURL)
		if err != nil {
			return fmt.Errorf("could not connect to db: %v", err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			return fmt.Errorf("could not ping db: %v", err)
		}

		pqStore, err := pqstorage.New(db, fs)
		if err != nil {
			return fmt.Errorf("could not create new pq storage: %v", err)
		}

		profileRepo = profile.NewRepository(log, pqStore)
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
