package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/profefe/profefe/pkg/config"
	"github.com/profefe/profefe/pkg/logger"
	"github.com/profefe/profefe/pkg/profile"
	pgstorage "github.com/profefe/profefe/pkg/storage/postgres"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("no command passed")
	}

	var run func(ctx context.Context, args []string) error
	switch cmd := strings.ToLower(os.Args[1]); cmd {
	case "import":
		run = runImport
	default:
		log.Fatalf("bad command %q", cmd)
	}

	if err := run(context.Background(), os.Args[2:]); err != nil {
		log.Fatal(err)
	}
}

func newProfileRepo(log *logger.Logger, conf config.Config) (*profile.Repository, error) {
	db, err := sql.Open("postgres", conf.Postgres.ConnString())
	if err != nil {
		return nil, fmt.Errorf("could not connect to db: %v", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("could not ping db: %v", err)
	}

	pgStorage, err := pgstorage.New(log.With("svc", "db"), db)
	if err != nil {
		return nil, fmt.Errorf("could not create new pg storage: %v", err)
	}

	return profile.NewRepository(log, pgStorage), nil
}
