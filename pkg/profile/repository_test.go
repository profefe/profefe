package profile_test

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage/inmemory"
)

func TestRepository_CreateProfile(t *testing.T) {
	st := inmemory.New()
	repo := profile.NewRepository(st)

	meta := map[string]interface{}{
		"service": "testapp",
		"type":    profile.CPUProfile.MarshalString(),
	}
	data, err := ioutil.ReadFile("../../testdata/test_cpu.prof")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	req := &profile.CreateProfileRequest{
		Meta: meta,
		Data: data,
	}
	newProf, err := repo.CreateProfile(ctx, req)
	if err != nil {
		t.Fatal(err)
	}

	_, err = st.Get(ctx, newProf.Digest)
	if err != nil {
		t.Fatal(err)
	}
}
