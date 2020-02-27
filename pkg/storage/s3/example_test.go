package s3

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

type mockS3Client struct {
	s3iface.S3API
}

type mockUploaderAPI struct {
	s3manageriface.UploaderAPI

	err    error
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
	testLogger := zaptest.NewLogger(t)
	st := New(log.New(testLogger), &mockS3Client{}, "test-bucket")
	st.uploader = &mockUploaderAPI{}

	params := &storage.WriteProfileParams{
		Service: "attribution_worker",
		Type:    profile.TypeCPU,
		Labels: profile.Labels{
			{"region", "europe"},
			{"datacenter", "esh"},
			{"hostname", "esh-attribution-10.adjust.com"},
			{"version", "1.2.0-git128d67636923c89e64b525e7d3ffb0df607c17bd"},
		},
		CreatedAt: time.Now().UTC(),
	}
	r := strings.NewReader("cpu1.prof")

	meta, err := st.WriteProfile(context.Background(), params, r)
	require.NoError(t, err)

	t.Logf("meta: %v\n", meta)
}
