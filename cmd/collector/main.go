package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/profefe/profefe/pkg/store"
	"github.com/profefe/profefe/pkg/store/inmemory"
)

const addr = ":10100"

func main() {
	ctx, cancelCtx := context.WithCancel(context.Background())

	sb := inmemory.NewRepo()

	s, err := store.NewStore(sb)
	if err != nil {
		log.Fatalf("could not create new store: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/api/v1/profile", newProfileHandler(s))

	server := http.Server{
		Addr:    addr,
		Handler: mux,
	}

	errc := make(chan error, 1)
	go func() {
		log.Printf("server is running on %s\n", addr)
		errc <- server.ListenAndServe()
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT)

	select {
	case <-sigs:
		cancelCtx()
	case <-ctx.Done():
		log.Println("stopping server")
		server.Shutdown(ctx)
	case <-errc:
		log.Fatalf("terminated: %v", err)
	}
}

func handleError(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), http.StatusServiceUnavailable)
}
