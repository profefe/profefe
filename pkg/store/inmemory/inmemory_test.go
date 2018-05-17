package inmemory

import (
	"context"
	"reflect"
	"testing"

	"github.com/profefe/profefe/pkg/store"
)

func TestRepo_ByName(t *testing.T) {
	repo := NewRepo()

	ctx := context.Background()

	p := &store.Profile{
		ID:     "t123",
		Name:   "test",
		Digest: "0xt123",
	}
	if err := repo.Create(ctx, p); err != nil {
		t.Fatal(err)
	}

	gotP, err := repo.ByName(ctx, "test")
	if err != nil {
		t.Fatal(err)
	} else if len(gotP) != 1 {
		t.Fatalf("ByName: got %d results", len(gotP))
	}
	if !reflect.DeepEqual(gotP[0], p) {
		t.Errorf("ByName: got %+v, want +%v", gotP[0], p)
	}
}
