package s3io

import (
	"context"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
)

type S3Conf struct {
	awsID           string
	awsSecret       string
	awsRegion       string
	endpoint        string
	checksumDisable bool
	disableSSL      bool
	pathStyle       bool
	partSize        int64
	concurrency     int
}

func New(opts ...Option) *S3Conf {
	s := &S3Conf{
		partSize:    64 * 1024 * 1024, // 64B default chunksize
		concurrency: 1,                // 1 default concurrency
	}

	for _, opt := range opts {
		opt(s)
	}
	return s
}

type Option func(*S3Conf)

func WithAccess(ak string) Option {
	return func(s *S3Conf) { s.awsID = ak }
}
func WithSecret(sk string) Option {
	return func(s *S3Conf) { s.awsSecret = sk }
}
func WithRegion(r string) Option {
	return func(s *S3Conf) { s.awsRegion = r }
}
func WithEndpoint(e string) Option {
	return func(s *S3Conf) { s.endpoint = e }
}
func WithDisableChecksum() Option {
	return func(s *S3Conf) { s.checksumDisable = true }
}
func WithDisableSSL() Option {
	return func(s *S3Conf) { s.disableSSL = true }
}
func WithPathStyle() Option {
	return func(s *S3Conf) { s.pathStyle = true }
}
func WithPartSize(p int64) Option {
	return func(s *S3Conf) { s.partSize = p }
}
func WithConcurrency(c int) Option {
	return func(s *S3Conf) { s.concurrency = c }
}

func (c *S3Conf) getCreds() credentials.StaticCredentialsProvider {
	// TODO support token/IAM
	if c.awsID == "" {
		c.awsID = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	if c.awsID == "" {
		log.Fatal("no AWS_ACCESS_KEY_ID found")
	}
	if c.awsSecret == "" {
		c.awsSecret = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	if c.awsSecret == "" {
		log.Fatal("no AWS_SECRET_ACCESS_KEY found")
	}

	return credentials.NewStaticCredentialsProvider(c.awsID, c.awsSecret, "")
}

func (c *S3Conf) ResolveEndpoint(service, region string) (aws.Endpoint, error) {
	return aws.Endpoint{
		PartitionID:       "aws",
		URL:               c.endpoint,
		SigningRegion:     c.awsRegion,
		HostnameImmutable: true,
	}, nil
}

func (c *S3Conf) config() aws.Config {
	creds := c.getCreds()

	if c.checksumDisable {
		cfg, err := config.LoadDefaultConfig(
			context.TODO(),
			config.WithRegion(c.awsRegion),
			config.WithCredentialsProvider(creds),
			config.WithEndpointResolver(c),
			config.WithAPIOptions([]func(*middleware.Stack) error{v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware}),
		)
		if err != nil {
			log.Fatalln("error:", err)
		}

		return cfg
	}
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(c.awsRegion),
		config.WithCredentialsProvider(creds),
		config.WithEndpointResolver(c),
	)
	if err != nil {
		log.Fatalln("error:", err)
	}

	return cfg
}

func (c *S3Conf) UploadData(r io.Reader, bucket, object string) error {
	uploader := manager.NewUploader(s3.NewFromConfig(c.config()))
	uploader.PartSize = c.partSize
	uploader.Concurrency = c.concurrency

	upinfo := &s3.PutObjectInput{
		Body:   r,
		Bucket: &bucket,
		Key:    &object,
	}

	_, err := uploader.Upload(context.Background(), upinfo)
	return err
}

func (c *S3Conf) DownloadData(w io.WriterAt, bucket, object string) (int64, error) {
	downloader := manager.NewDownloader(s3.NewFromConfig(c.config()))
	downloader.PartSize = c.partSize
	downloader.Concurrency = c.concurrency

	downinfo := &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &object,
	}

	return downloader.Download(context.Background(), w, downinfo)
}
