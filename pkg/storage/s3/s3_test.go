package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
)

func Test_key(t *testing.T) {
	tests := []struct {
		name string
		meta profile.Meta
		want string
	}{
		{
			name: "multiple labels",
			meta: profile.Meta{
				ProfileID:  profile.ID("1"),
				Service:    "svc1",
				Type:       profile.CPUProfile,
				InstanceID: profile.InstanceID("1"),
				Labels: profile.Labels{
					profile.Label{
						Key:   "k1",
						Value: "v1",
					},
					profile.Label{
						Key:   "k2",
						Value: "v2",
					},
				},
				CreatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
			},
			want: "svc1/cpu/1257894000000000000/k1=v1,k2=v2/64",
		},
		{
			name: "no labels",
			meta: profile.Meta{
				ProfileID:  profile.ID("1"),
				Service:    "svc1",
				Type:       profile.CPUProfile,
				InstanceID: profile.InstanceID("1"),
				CreatedAt:  time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
			},
			want: "svc1/cpu/1257894000000000000//64",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := key(tt.meta); got != tt.want {
				t.Errorf("path() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_meta(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		want    *profile.Meta
		wantErr bool
	}{
		{
			name:    "error when the key does not have the correct splits",
			key:     "invalidkey",
			wantErr: true,
		},
		{
			name:    "error when id is an invalid format",
			key:     "svc1/cpu/1257894000000000000/k1=v1,k2=v2/zz",
			wantErr: true,
		},
		{
			name:    "error when label encoding is incorrect",
			key:     "svc1/cpu/1257894000000000000/%GG=v1/64",
			wantErr: true,
		},
		{
			name:    "error when time is an invalid format",
			key:     "svc1/cpu/badint/k1=v1,k2=v2/64",
			wantErr: true,
		},
		{
			name: "able to parse",
			key:  "svc1/cpu/1257894000000000000/k1=v1,k2=v2/64",
			want: &profile.Meta{
				ProfileID: profile.ID("1"),
				Service:   "svc1",
				Type:      profile.CPUProfile,
				Labels: profile.Labels{
					profile.Label{
						Key:   "k1",
						Value: "v1",
					},
					profile.Label{
						Key:   "k2",
						Value: "v2",
					},
				},
				CreatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := meta(tt.key)
			if (err != nil) != tt.wantErr {
				t.Fatalf("meta() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("meta() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_includes(t *testing.T) {
	tests := []struct {
		name string
		a    profile.Labels
		b    profile.Labels
		want bool
	}{
		{
			name: "empty a includes empty b",
			a:    profile.Labels{},
			b:    profile.Labels{},
			want: true,
		},
		{
			name: "a includes empty b",
			a: profile.Labels{
				{
					Key:   "k1",
					Value: "v1",
				},
			},
			b:    profile.Labels{},
			want: true,
		},
		{
			name: "a includes b",
			a: profile.Labels{
				{
					Key:   "k1",
					Value: "v1",
				},
				{
					Key:   "k2",
					Value: "v2",
				},
			},
			b: profile.Labels{
				{
					Key:   "k1",
					Value: "v1",
				},
			},
			want: true,
		},
		{
			name: "a does NOT include all of  b",
			a: profile.Labels{
				{
					Key:   "k1",
					Value: "v1",
				},
			},
			b: profile.Labels{
				{
					Key:   "k1",
					Value: "v1",
				},
				{
					Key:   "k2",
					Value: "v2",
				},
			},
			want: false,
		},
		{
			name: "includes same key but different value",
			a: profile.Labels{
				{
					Key:   "k1",
					Value: "v1",
				},
			},
			b: profile.Labels{
				{
					Key:   "k1",
					Value: "v2",
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := includes(tt.a, tt.b); got != tt.want {
				t.Errorf("includes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_profilePath(t *testing.T) {
	got := profilePath(profile.ID("1"))
	want := "v0/profiles/64"
	if got != want {
		t.Errorf("profilePath() = %s, want %s", got, want)
	}
}

func Test_prefix(t *testing.T) {
	params := &storage.FindProfilesParams{
		Service:      "svc1",
		Type:         profile.CPUProfile,
		CreatedAtMin: time.Unix(0, 0),
		CreatedAtMax: time.Now(),
		Limit:        10,
	}

	got := prefix(params)
	want := "svc1/cpu"
	if got != want {
		t.Errorf("prefix() = %s, want %s", got, want)
	}
}

func Test_startAfter(t *testing.T) {
	params := &storage.FindProfilesParams{
		Service:      "svc1",
		Type:         profile.CPUProfile,
		CreatedAtMin: time.Unix(0, 0),
		CreatedAtMax: time.Now(),
		Limit:        10,
	}

	got := startAfter(params)
	want := "svc1/cpu/0"
	if got != want {
		t.Errorf("startAfter() = %s, want %s", got, want)
	}
}

type mockUploaderAPI struct {
	s3manageriface.UploaderAPI
	err error

	inputs []*s3manager.UploadInput
}

func (m *mockUploaderAPI) UploadWithContext(ctx aws.Context, input *s3manager.UploadInput, _ ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
	if m.err != nil {
		return nil, m.err
	}
	m.inputs = append(m.inputs, input)
	return nil, nil
}

func TestStore_WriteProfile(t *testing.T) {
	type inputs struct {
		bucket string
		body   string
	}
	tests := []struct {
		name     string
		uploader *mockUploaderAPI
		bucket   string
		meta     profile.Meta
		r        *strings.Reader
		want     []inputs
		wantErr  bool
	}{
		{
			name:     "test writing data into mock",
			uploader: &mockUploaderAPI{err: nil},
			bucket:   "b1",
			meta: profile.Meta{
				ProfileID:  []byte("1"),
				Service:    "svc1",
				Type:       profile.CPUProfile,
				InstanceID: []byte("2"),
				Labels: profile.Labels{
					{
						Key:   "k1",
						Value: "v1",
					},
				},
			},
			r: strings.NewReader("profile"),
			want: []inputs{
				{"b1", `{"meta":{"profile_id":"64","service":"svc1","type":1,"instance_id":"Mg==","labels":[{"key":"k1","value":"v1"}],"created_at":"0001-01-01T00:00:00Z"},"path":"v0/profiles/64"}`},
				{"b1", "profile"},
			},
		},
		{
			name: "error from AWS returns error",
			uploader: &mockUploaderAPI{
				err: fmt.Errorf("error"),
			},
			meta:    profile.Meta{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				uploader: tt.uploader,
				S3Bucket: tt.bucket,
			}
			if err := s.WriteProfile(context.Background(), tt.meta, tt.r); (err != nil) != tt.wantErr {
				t.Errorf("Store.WriteProfile() error = %v, wantErr %v", err, tt.wantErr)
			}

			got := tt.uploader.inputs
			if len(got) != len(tt.want) {
				t.Fatalf("WriteProfile() = %v, want %v", got, tt.want)
			}

			for i := range got {
				if *got[i].Bucket != tt.want[i].bucket {
					t.Errorf("WriteProfile().[%d].Bucket = %v, want %v", i, *got[i].Bucket, tt.want[i].bucket)
				}
				b, _ := ioutil.ReadAll(got[i].Body)
				if string(b) != tt.want[i].body {
					t.Errorf("WriteProfile().[%d].Body = %v, want %v", i, string(b), tt.want[i].body)
				}

			}
		})
	}
}

type mockDownloaderAPI struct {
	s3manageriface.DownloaderAPI
	err error
}

func (m *mockDownloaderAPI) DownloadWithContext(ctx aws.Context, wa io.WriterAt, input *s3.GetObjectInput, _ ...func(*s3manager.Downloader)) (int64, error) {
	b, err := mockProfile()
	if err != nil {
		return 0, err
	}
	n, err := wa.WriteAt(b, 0)
	if err != nil {
		return 0, err
	}
	return int64(n), m.err
}

func mockProfile() ([]byte, error) {
	p := &pprofProfile.Profile{}
	buf := &bytes.Buffer{}
	err := p.WriteUncompressed(buf)
	return buf.Bytes(), err
}

func TestStore_ListProfiles(t *testing.T) {
	t.Run("download two profiles", func(t *testing.T) {
		s := &Store{
			S3Bucket:   "b1",
			downloader: &mockDownloaderAPI{},
		}
		itr, err := s.ListProfiles(
			context.Background(),
			[]profile.ID{[]byte("1"), []byte("2")},
		)
		if err != nil {
			t.Errorf("Store.ListProfiles() error = %v", err)
			return
		}
		count := 0
		profiles := []*pprofProfile.Profile{}
		for itr.Next() {
			count++
			p, err := itr.Profile()
			if err != nil {
				t.Errorf("Store.ListProfiles().Profile() error = %v", err)
			}
			profiles = append(profiles, p)
		}
		if count != 2 {
			t.Errorf("Store.ListProfiles().Next() = %d, want %d ", count, 2)
		}
	})
}
