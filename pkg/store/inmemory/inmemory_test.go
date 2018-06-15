package inmemory

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/profefe/profefe/pkg/profile"
)

func TestRepo_Create(t *testing.T) {
	ctx := context.Background()
	ts := time.Now().Add(-5 * time.Second)
	meta := map[string]interface{}{
		"service":    "test_svc",
		"id":         "id123",
		"generation": "gen123",
		"type":       profile.CPUProfile.MarshalString(),
		"ts":         ts.Format(time.RFC3339),
	}

	data := make([]byte, 4)
	rand.Read(data)

	repo := New()

	p, err := repo.Create(ctx, meta, data)
	if err != nil {
		t.Fatal(err)
	}
	if p == nil {
		t.Fatal("Create: unexpected empty profile")
	}

	if got, want := p.Service, "test_svc"; got != want {
		t.Errorf("service: got %v, want %v", got, want)
	}
	if got, want := p.BuildID, "id123"; got != want {
		t.Errorf("build_id: got %v, want %v", got, want)
	}
	if got, want := p.Generation, "gen123"; got != want {
		t.Errorf("generation: got %v, want %v", got, want)
	}
	if got, want := p.Type, profile.CPUProfile; got != want {
		t.Errorf("type: got %v, want %v", got, want)
	}
	if got, want := p.CreatedAt, ts; got.Equal(want) {
		t.Errorf("created_at: got %v, want %v", got, want)
	}

	if p.Digest == "" {
		t.Error("digest: empty")
	}
	if got, want := p.Size, int64(4); got != want {
		t.Errorf("size: got %v, want %v", got, want)
	}
}
