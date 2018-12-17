package inmemory

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/profefe/profefe/pkg/profile"
)

func TestStorage_Create(t *testing.T) {
	st := New()

	meta := map[string]interface{}{
		"service": "testapp",
		"type":    profile.CPUProfile.MarshalString(),
	}
	inProf, profReader := profile.NewTestProfile(t, "../../../testdata/test_cpu.prof", meta)

	err := st.Create(context.Background(), inProf, profReader)
	if err != nil {
		t.Fatal(err)
	}

	if inProf.Digest == "" {
		t.Error("digest empty")
	}
	if inProf.Size == 0 {
		t.Errorf("size empty")
	}
}

func TestStorage_Open(t *testing.T) {
	st, inProf := setupTestStorage(t)

	outProfReader, err := st.Open(context.Background(), inProf.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if outProfReader == nil {
		t.Fatal("Open: no profile")
	}

	_, err = st.Open(context.Background(), "blah")
	if err != collector.ErrNotFound {
		t.Fatalf("Open: got %v, want not found", err)
	}
}

func TestStorage_Get(t *testing.T) {
	st, inProf := setupTestStorage(t)

	outProf, err := st.Get(context.Background(), inProf.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if outProf.Digest != inProf.Digest {
		t.Fatalf("Get: got %#v, want %#v", inProf, outProf)
	}

	_, err = st.Get(context.Background(), "blah")
	if err != collector.ErrNotFound {
		t.Fatalf("Get: got %v, want not found", err)
	}
}

func TestStorage_Query(t *testing.T) {
	st, inProf := setupTestStorage(t)

	cases := []struct {
		query   profile.QueryRequest
		wantErr bool
	}{
		{
			profile.QueryRequest{
				Digest: inProf.Digest,
			},
			false,
		},
		{
			profile.QueryRequest{
				Service: inProf.Service,
			},
			false,
		},
		{
			profile.QueryRequest{
				Type: inProf.Type,
			},
			false,
		},
		{
			profile.QueryRequest{
				CreatedAtMin: inProf.CreatedAt.Add(-1 * time.Minute),
			},
			false,
		},
		{
			profile.QueryRequest{
				CreatedAtMax: inProf.CreatedAt.Add(time.Minute),
			},
			false,
		},
		{
			profile.QueryRequest{
				Labels: profile.Labels{
					{"foo", "bar"},
				},
			},
			false,
		},
		// fail cases
		{
			profile.QueryRequest{
				Digest: "blah",
			},
			true,
		},
	}

	for n, tc := range cases {
		t.Run(fmt.Sprintf("case %d", n), func(t *testing.T) {
			outProfs, err := st.Query(context.Background(), &tc.query)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Query: got %v, want error %v", err, tc.wantErr)
			}

			if tc.wantErr {
				return
			} else if got := len(outProfs); got != 1 {
				t.Fatalf("Query: got %d profiles, want 1", got)
			} else if got := outProfs[0]; got != inProf {
				t.Fatalf("Query: got %#v, want %#v", got, inProf)
			}
		})
	}
}

func TestStorage_Delete(t *testing.T) {
	st, inProf := setupTestStorage(t)

	err := st.Delete(context.Background(), inProf.Digest)
	if err != nil {
		t.Fatal(err)
	}

	_, err = st.Get(context.Background(), inProf.Digest)
	if err != collector.ErrNotFound {
		t.Fatalf("Get: got %v after Delete, want not found", err)
	}

	err = st.Delete(context.Background(), inProf.Digest)
	if err != nil {
		t.Fatal(err)
	}
}

func setupTestStorage(t testing.TB) (*Storage, *profile.Profile) {
	st := New()

	meta := map[string]interface{}{
		"service": "testapp",
		"foo":     "bar",
		"type":    profile.CPUProfile.MarshalString(),
	}
	inProf, profReader := profile.NewTestProfile(t, "../../../testdata/test_cpu.prof", meta)

	err := st.Create(context.Background(), inProf, profReader)
	if err != nil {
		t.Fatal(err)
	}

	return st, inProf
}
