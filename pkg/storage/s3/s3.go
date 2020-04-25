package s3

import (
	"flag"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/profefe/profefe/pkg/log"
)

const (
	defaultRegion     = "us-east-1"
	defaultMaxRetries = 3
)

type Config struct {
	EndpointURL string
	DisableSSL  bool
	Region      string
	Bucket      string
	MaxRetries  int
}

func (conf *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&conf.EndpointURL, "s3.endpoint-url", "", "override default URL to s3 service")
	f.BoolVar(&conf.DisableSSL, "s3.disable-ssl", false, "disable SSL verification")
	f.StringVar(&conf.Region, "s3.region", defaultRegion, "object storage region")
	f.StringVar(&conf.Bucket, "s3.bucket", "", "s3 bucket profile destination")
	f.IntVar(&conf.MaxRetries, "s3.max-retries", defaultMaxRetries, "s3 request maximum number of retries")
}

func (conf *Config) CreateStorage(logger *log.Logger) (*Storage, error) {
	var forcePathStyle bool
	if conf.EndpointURL != "" {
		// should one use custom object storage service (e.g. Minio), path-style addressing needs to be set
		forcePathStyle = true
	}
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(conf.EndpointURL),
		DisableSSL:       aws.Bool(conf.DisableSSL),
		Region:           aws.String(conf.Region),
		MaxRetries:       aws.Int(conf.MaxRetries),
		S3ForcePathStyle: aws.Bool(forcePathStyle),
	})
	if err != nil {
		return nil, fmt.Errorf("could not create s3 session: %w", err)
	}
	return NewStorage(logger, s3.New(sess), conf.Bucket), nil
}
