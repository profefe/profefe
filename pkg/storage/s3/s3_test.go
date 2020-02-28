package s3

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

//func Test_key(t *testing.T) {
//	tests := []struct {
//		name string
//		meta profile.Meta
//		want string
//	}{
//		{
//			name: "multiple labels",
//			meta: profile.Meta{
//				ProfileID: profile.ID("1"),
//				Service:   "svc1",
//				Type:      profile.TypeCPU,
//				Labels: profile.Labels{
//					profile.Label{
//						Key:   "k1",
//						Value: "v1",
//					},
//					profile.Label{
//						Key:   "k2",
//						Value: "v2",
//					},
//				},
//				CreatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
//			},
//			want: "svc1/cpu/1257894000000000000/k1=v1,k2=v2/64",
//		},
//		{
//			name: "no labels",
//			meta: profile.Meta{
//				ProfileID: profile.ID("1"),
//				Service:   "svc1",
//				Type:      profile.TypeCPU,
//				CreatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
//			},
//			want: "svc1/cpu/1257894000000000000//64",
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			if got := key(tt.meta); got != tt.want {
//				t.Errorf("path() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}

func TestMetaFromProfileKey(t *testing.T) {
	cases := []struct {
		name    string
		key     string
		want    profile.Meta
		wantErr bool
	}{
		{
			name:    "error when the key does not have the correct splits",
			key:     "invalidkey",
			wantErr: true,
		},
		{
			name:    "error when key doesn't contain schema",
			key:     "svc1/1/9bsv0s3ipt32jfck6kt0/k1=v1,k2=v2",
			wantErr: true,
		},
		{
			name:    "error when type is incorrect",
			key:     "P0.svc1/cpu/9bsv0s3ipt32jfck6kt0/k1=v1,k2=v2",
			wantErr: true,
		},
		{
			name:    "error when type is unknown",
			key:     "P0.svc1/0/9bsv0s3ipt32jfck6kt0/k1=v1,k2=v2",
			wantErr: true,
		},
		{
			name:    "error when digest is an invalid format",
			key:     "P0.svc1/1/b49int/k1=v1,k2=v2",
			wantErr: true,
		},
		{
			name:    "error when label encoding is incorrect",
			key:     "P0.svc1/1/9bsv0s3ipt32jfck6kt0/%GG=v1",
			wantErr: true,
		},
		{
			name: "valid key",
			key:  "P0.svc1/1/9bsv0s3ipt32jfck6kt0/k1=v1,k2=v2",
			want: profile.Meta{
				ProfileID: profile.ID("P0.svc1/1/9bsv0s3ipt32jfck6kt0/k1=v1,k2=v2"),
				Service:   "svc1",
				Type:      profile.TypeCPU,
				Labels: profile.Labels{
					{"k1", "v1"},
					{"k2", "v2"},
				},
				CreatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
			},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := metaFromProfileKey(profefeSchema, tt.key)
			require.Equal(t, tt.wantErr, err != nil, "error = %v, wantErr %v", err, tt.wantErr)
			assert.Equal(t, tt.want, got)
		})
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

func TestStorage_WriteProfile(t *testing.T) {
	type inputs struct {
		bucket string
		body   string
	}
	tests := []struct {
		name     string
		uploader *mockUploaderAPI
		bucket   string
		params   *storage.WriteProfileParams
		r        *strings.Reader
		want     []inputs
		wantErr  bool
	}{
		{
			name:     "write data into mock",
			uploader: &mockUploaderAPI{err: nil},
			bucket:   "b1",
			params: &storage.WriteProfileParams{
				Service: "svc1",
				Type:    profile.TypeCPU,
				Labels: profile.Labels{
					{"k1", "v1"},
					{"k2", "v2"},
				},
				CreatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
			},
			r: strings.NewReader("profile"),
			want: []inputs{
				{
					bucket: "b1",
					body:   "profile",
				},
			},
		},
		{
			name: "error from AWS returns error",
			uploader: &mockUploaderAPI{
				err: fmt.Errorf("error"),
			},
			params:  &storage.WriteProfileParams{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Storage{
				logger:   log.New(zaptest.NewLogger(t)),
				uploader: tt.uploader,
				bucket:   tt.bucket,
			}
			_, err := s.WriteProfile(context.Background(), tt.params, tt.r)
			require.Equal(t, tt.wantErr, err != nil, "error = %v, wantErr %v", err, tt.wantErr)
			require.Len(t, tt.uploader.inputs, len(tt.want))

			for i, input := range tt.uploader.inputs {
				assert.Equal(t, tt.want[i].bucket, *input.Bucket)
				_, err := metaFromProfileKey(profefeSchema, *input.Key)
				require.NoError(t, err)
				b, _ := ioutil.ReadAll(input.Body)
				assert.Equal(t, tt.want[i].body, string(b))
			}
		})
	}
}

type mockDownloaderAPI struct {
	s3manageriface.DownloaderAPI
	err error
}

func (m *mockDownloaderAPI) DownloadWithContext(ctx aws.Context, wa io.WriterAt, input *s3.GetObjectInput, _ ...func(*s3manager.Downloader)) (int64, error) {
	b := make([]byte, 0)
	n, err := wa.WriteAt(b, 0)
	if err != nil {
		return 0, err
	}
	return int64(n), m.err
}

func TestStorage_ListProfiles(t *testing.T) {
	t.Run("download two profiles", func(t *testing.T) {
		s := &Storage{
			bucket:     "b1",
			logger:     log.New(zaptest.NewLogger(t)),
			downloader: &mockDownloaderAPI{},
		}
		ids := []profile.ID{"1", "2"}

		itr, err := s.ListProfiles(context.Background(), ids)
		require.NoError(t, err)

		defer itr.Close()

		count := 0
		var profiles []io.Reader
		for itr.Next() {
			count++
			pr, err := itr.Profile()
			require.NoError(t, err)

			profiles = append(profiles, pr)
		}

		assert.Equal(t, len(ids), count, "must have found %d profiles", len(ids))
		assert.Len(t, profiles, len(ids))
	})
}

type mockService struct {
	s3iface.S3API

	// data to send to ListObjectsV2PagesWithContext
	page s3.ListObjectsV2Output
	err  error

	// data sent from ListObjectsV2PagesWithContext
	input *s3.ListObjectsV2Input

	// data sent from page function
	nextPage bool
}

func (s *mockService) ListObjectsV2PagesWithContext(ctx aws.Context, input *s3.ListObjectsV2Input, fn func(*s3.ListObjectsV2Output, bool) bool, opts ...request.Option) error {
	s.input = input
	s.nextPage = fn(&s.page, false)
	return s.err
}

func Test_FindProfileIDs(t *testing.T) {
	s := &Storage{
		bucket: "b1",
		logger: log.New(zaptest.NewLogger(t)),
	}

	t.Run("no service returns error", func(t *testing.T) {
		params := &storage.FindProfilesParams{}
		_, err := s.FindProfileIDs(context.Background(), params)
		require.Error(t, err)
	})

	t.Run("no created-at returns error", func(t *testing.T) {
		params := &storage.FindProfilesParams{
			Service: "svc1",
		}
		_, err := s.FindProfileIDs(context.Background(), params)
		require.Error(t, err)
	})

	t.Run("no s3 objects returns not found error", func(t *testing.T) {
		s.svc = &mockService{}
		params := &storage.FindProfilesParams{
			Service:      "svc1",
			CreatedAtMin: time.Unix(0, 0),
		}
		_, err := s.FindProfileIDs(context.Background(), params)
		require.Equal(t, storage.ErrNotFound, err)
	})

	t.Run("s3 object with incorrectly formatted key is not returned", func(t *testing.T) {
		s.svc = &mockService{
			page: s3.ListObjectsV2Output{
				Contents: []*s3.Object{
					{
						Key: aws.String("incorrect_key_format"),
					},
				},
				IsTruncated: aws.Bool(false),
			},
		}
		params := &storage.FindProfilesParams{
			Service:      "svc1",
			CreatedAtMin: time.Unix(0, 0),
			Labels: profile.Labels{
				profile.Label{
					Key:   "k1",
					Value: "v1",
				},
			},
		}
		_, err := s.FindProfileIDs(context.Background(), params)
		require.Equal(t, err, storage.ErrNotFound)
	})

	t.Run("s3 object with profile found", func(t *testing.T) {
		profileKey := "P0.svc1/1/bpc00mript33iv4net00/k1=v1,k2=v2"

		s.svc = &mockService{
			page: s3.ListObjectsV2Output{
				Contents: []*s3.Object{
					{
						Key: aws.String(profileKey),
					},
				},
				IsTruncated: aws.Bool(false),
			},
		}
		params := &storage.FindProfilesParams{
			Service:      "svc1",
			CreatedAtMin: time.Unix(0, 0),
		}
		ids, err := s.FindProfileIDs(context.Background(), params)
		require.NoError(t, err)

		require.Len(t, ids, 1)
		assert.Equal(t, profileKey, string(ids[0]))
	})

	t.Run("s3 object after max time not returned", func(t *testing.T) {
		s.svc = &mockService{
			page: s3.ListObjectsV2Output{
				Contents: []*s3.Object{
					{
						Key: aws.String("P0.svc1/1/bpc00mript33iv4net00/k1=v1,k2=v2"),
					},
				},
				IsTruncated: aws.Bool(false),
			},
		}
		params := &storage.FindProfilesParams{
			Service:      "svc1",
			CreatedAtMin: time.Unix(0, 0),
			CreatedAtMax: time.Unix(0, 0),
		}

		_, err := s.FindProfileIDs(context.Background(), params)
		require.Equal(t, storage.ErrNotFound, err)
	})
}
