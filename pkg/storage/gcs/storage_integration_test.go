package gcs_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/profefe/profefe/pkg/log"
	storageGCS "github.com/profefe/profefe/pkg/storage/gcs"
	"github.com/profefe/profefe/pkg/storage/storagetest"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"google.golang.org/api/option"
)

type roundTripper url.URL

// This is a workaround. When using an emulator, it is not possible to use the same client object for both uploading and other operations.
// https://github.com/googleapis/google-cloud-go/issues/2476
func (rt roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Host = rt.Host
	req.URL.Host = rt.Host
	req.URL.Scheme = rt.Scheme
	return http.DefaultTransport.RoundTrip(req)
}

func TestStorage(t *testing.T) {
	emulatorURL := os.Getenv("GCS_EMULATOR_ENDPOINT_URL")
	if emulatorURL == "" {
		t.SkipNow()
	}

	u, _ := url.Parse(emulatorURL)
	hClient := &http.Client{Transport: roundTripper(*u)}
	ctx := context.Background()
	client, err := gcs.NewClient(ctx, option.WithHTTPClient(hClient), option.WithoutAuthentication())
	require.NoError(t, err)

	gcsBucket := fmt.Sprintf("profefe-test.%d", time.Now().Unix())
	setupGCSBucket(ctx, t, client, gcsBucket)

	testLogger := zaptest.NewLogger(t, zaptest.Level(zapcore.FatalLevel))
	st := storageGCS.NewStorage(log.New(testLogger), client, gcsBucket)

	t.Run("Reader", func(t *testing.T) {
		ts := &storagetest.ReaderTestSuite{
			Reader: st,
			Writer: st,
		}
		suite.Run(t, ts)
	})

	t.Run("Writer", func(t *testing.T) {
		ts := &storagetest.WriterTestSuite{
			Reader: st,
			Writer: st,
		}
		suite.Run(t, ts)
	})
}

func setupGCSBucket(ctx context.Context, t *testing.T, client *gcs.Client, bucket string) {
	t.Logf("setup s3 bucket %q", bucket)

	err := client.Bucket(bucket).Create(ctx, "profefe-test", nil)
	require.NoError(t, err)

	t.Cleanup(func() {
		// TODO delete the bucket when the issue is resolved. https://github.com/fsouza/fake-gcs-server/issues/214
		// err := client.Bucket(bucket).Delete(ctx)
		// require.NoError(t, err)
	})
}
