package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/profefe/profefe/pkg/store"
)

type profileHandler struct {
	store *store.Store
}

func newProfileHandler(s *store.Store) *profileHandler {
	return &profileHandler{
		store: s,
	}
}

func (c *profileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "405 method not allowed", http.StatusMethodNotAllowed)
		return
	}

	log.Printf("request method=%s url=%q\n", r.Method, r.URL)

	switch r.URL.Path {
	case "/api/v1/profile":
		c.handleProfile(w, r)
	default:
		http.NotFound(w, r)
		return
	}
}

type ProfileRequest struct {
	Meta map[string]string
	Data []byte
}

func (c *profileHandler) handleProfile(w http.ResponseWriter, r *http.Request) {
	var preq ProfileRequest
	if err := json.NewDecoder(r.Body).Decode(&preq); err != nil {
		handleError(w, err)
		return
	}

	p, err := c.store.Save(r.Context(), preq.Meta, preq.Data)
	if err != nil {
		handleError(w, err)
		return
	}

	log.Printf("DEBUG profile: %+v\n", p)

	fmt.Fprintln(w, "OK")
}
