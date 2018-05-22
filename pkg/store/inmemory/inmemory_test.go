package inmemory

import (
	"context"
	"reflect"
	"testing"

	"github.com/profefe/profefe/pkg/profile"
)

func TestRepo_ByName(t *testing.T) {
	repo := NewRepo()

	ctx := context.Background()

	p := &profile.Profile{
		Service: "test",
		BuildID: "t123",
		Digest:  "0xt123",
	}
	if err := repo.Create(ctx, p); err != nil {
		t.Fatal(err)
	}

	gotP, err := repo.ByService(ctx, "test")
	if err != nil {
		t.Fatal(err)
	} else if len(gotP) != 1 {
		t.Fatalf("ByService: got %d results", len(gotP))
	}
	if !reflect.DeepEqual(gotP[0], p) {
		t.Errorf("ByService: got %+v, want +%v", gotP[0], p)
	}
}
