package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/profefe/profefe/cmd/collector/app"
	"github.com/profefe/profefe/cmd/collector/middleware"
	"github.com/profefe/profefe/pkg/filestore"
	"github.com/profefe/profefe/pkg/store"
	pqstore "github.com/profefe/profefe/pkg/store/postgres"

	_ "github.com/lib/pq"
)

const addr = ":10100"

const (
	defaultDataRoot = "/tmp/profefe"
	postgresUser    = "postgres"
	postgresHost    = "127.0.0.1"
	postgresDB      = "profiles"
)

func main() {
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	var svc *app.ProfileService
	{
		fileStore, err := filestore.New(defaultDataRoot)
		if err != nil {
			log.Fatalf("could not create file store: %v", err)
		}

		dbURL := fmt.Sprintf("postgres://%s@%s/%s?sslmode=disable", postgresUser, postgresHost, postgresDB)
		db, err := sql.Open("postgres", dbURL)
		if err != nil {
			log.Fatalf("could not connect to db: %v", err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			log.Fatalf("could not ping db: %v", err)
		}

		pqRepo, err := pqstore.New(db, fileStore)
		if err != nil {
			log.Fatalf("could not create new pq store: %v", err)
		}

		s := store.New(pqRepo)

		svc = app.NewProfileService(s)
	}

	mux := http.NewServeMux()
	apiHandler := app.NewAPIHandler(svc)
	apiHandler.RegisterRoutes(mux)

	handler := middleware.LoggingHandler(os.Stdout, mux)
	handler = middleware.RecoveryHandler(handler)

	server := http.Server{
		Addr:    addr,
		Handler: handler,
	}

	errc := make(chan error, 1)
	go func() {
		log.Printf("server is running on %s\n", addr)
		errc <- server.ListenAndServe()
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	select {
	case <-sigs:
		log.Println("exiting")
	case err := <-errc:
		if err != http.ErrServerClosed {
			log.Fatalf("terminated: %v", err)
		}
	}

	server.Shutdown(ctx)
}
