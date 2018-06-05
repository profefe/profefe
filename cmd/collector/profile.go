package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/profefe/profefe/pkg/profile"
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
	if r.Method == http.MethodPost {
		var preq ProfileRequest
		if err := json.NewDecoder(r.Body).Decode(&preq); err != nil {
			handleError(w, err)
			return
		}

		err := c.createProfile(r.Context(), &preq)
		if err != nil {
			handleError(w, err)
			return
		}

		fmt.Fprintln(w, "OK")
		return
	}

	meta := make(map[string]string)
	if v := r.URL.Query().Get("service"); v != "" {
		meta["service"] = v
	} else {
		http.Error(w, "no service", http.StatusBadRequest)
		return
	}
	if v := r.URL.Query().Get("type"); v != "" {
		meta["type"] = v
	}

	p, data, err := c.getProfile(r.Context(), meta)
	if err != nil {
		handleError(w, err)
		return
	}
	defer data.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, p.Type))

	io.Copy(w, data)
}

func (c *profileHandler) getProfile(ctx context.Context, meta map[string]string) (*profile.Profile, io.ReadCloser, error) {
	p, data, err := c.store.Find(ctx, meta)
	if err != nil {
		return nil, nil, err
	}

	log.Printf("DEBUG get profile: %+v\n", p)

	return p, data, nil
}

func (c *profileHandler) createProfile(ctx context.Context, req *ProfileRequest) error {
	p, err := c.store.Create(ctx, req.Meta, req.Data)
	if err != nil {
		return err
	}

	log.Printf("DEBUG create profile: %+v\n", p)

	return nil
}
