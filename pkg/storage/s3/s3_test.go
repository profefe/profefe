package s3

import (
	"reflect"
	"testing"
	"time"

	"github.com/profefe/profefe/pkg/profile"
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
			want: "/svc1/cpu/1257894000000000000/k1=v1,k2=v2/64",
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
			want: "/svc1/cpu/1257894000000000000//64",
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
			key:     "/svc1/cpu/1257894000000000000/k1=v1,k2=v2/zz",
			wantErr: true,
		},
		{
			name:    "error when label encoding is incorrect",
			key:     "/svc1/cpu/1257894000000000000/%GG=v1/64",
			wantErr: true,
		},
		{
			name:    "error when time is an invalid format",
			key:     "/svc1/cpu/badint/k1=v1,k2=v2/64",
			wantErr: true,
		},
		{
			name: "able to parse",
			key:  "/svc1/cpu/1257894000000000000/k1=v1,k2=v2/64",
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
