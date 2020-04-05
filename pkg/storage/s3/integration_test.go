package s3_test

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/profefe/profefe/pkg/log"
	storageS3 "github.com/profefe/profefe/pkg/storage/s3"
	"github.com/profefe/profefe/pkg/storage/storagetest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestStorage(t *testing.T) {
	endpointURL := os.Getenv("MINIO_ENDPOINT_URL")
	if endpointURL == "" {
		t.SkipNow()
	}

	s3Region := "eu-central-1"
	creds := credentials.NewStaticCredentials("minio", "minioadmin123", "")
	sess, err := session.NewSession(&aws.Config{
		Credentials: creds,

		Region:           aws.String(s3Region),
		Endpoint:         aws.String(endpointURL),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	})
	require.NoError(t, err)

	svc := s3.New(sess)

	s3Bucket := fmt.Sprintf("profefe-test.%d", time.Now().Unix())
	setupS3Bucket(t, svc, s3Bucket)

	testLogger := zaptest.NewLogger(t)
	st := storageS3.New(log.New(testLogger), svc, s3Bucket)

	storagetest.RunTestSuite(t, st)
}

func setupS3Bucket(t *testing.T, svc *s3.S3, bucket string) {
	t.Logf("setup s3 bucket %q", bucket)

	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	var aerr awserr.Error
	if errors.As(err, &aerr) {
		if aerr.Code() == s3.ErrCodeBucketAlreadyExists {
			err = nil
		}
	}
	require.NoError(t, err)

	err = svc.WaitUntilBucketExists(&s3.HeadBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	t.Cleanup(func() {
		objs, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)

		delInput := &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &s3.Delete{},
		}
		for _, obj := range objs.Contents {
			delInput.Delete.Objects = append(delInput.Delete.Objects, &s3.ObjectIdentifier{Key: obj.Key})
		}
		_, err = svc.DeleteObjects(delInput)
		require.NoError(t, err)

		_, err = svc.DeleteBucket(&s3.DeleteBucketInput{Bucket: aws.String(bucket)})
		require.NoError(t, err)
	})
}
