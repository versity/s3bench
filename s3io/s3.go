package s3io

import (
	"context"
	"crypto/tls"
	"io"
	"log"
	"net/http"
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
	pathStyle       bool
	partSize        int64
	concurrency     int
	debug           bool
	client          *s3.Client
}

func New(opts ...Option) *S3Conf {
	s := &S3Conf{
		partSize:    64 * 1024 * 1024, // 64B default chunksize
		concurrency: 1,                // 1 default concurrency
	}

	for _, opt := range opts {
		opt(s)
	}

	s.client = s3.NewFromConfig(s.config())

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
func WithPathStyle() Option {
	return func(s *S3Conf) { s.pathStyle = true }
}
func WithPartSize(p int64) Option {
	return func(s *S3Conf) { s.partSize = p }
}
func WithConcurrency(c int) Option {
	return func(s *S3Conf) { s.concurrency = c }
}
func WithDebug() Option {
	return func(s *S3Conf) { s.debug = true }
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

func (c *S3Conf) ResolveEndpoint(service, region string, options ...interface{}) (aws.Endpoint, error) {
	return aws.Endpoint{
		PartitionID:       "aws",
		URL:               c.endpoint,
		SigningRegion:     c.awsRegion,
		HostnameImmutable: true,
	}, nil
}

func (c *S3Conf) config() aws.Config {
	creds := c.getCreds()

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}

	opts := []func(*config.LoadOptions) error{
		config.WithRegion(c.awsRegion),
		config.WithCredentialsProvider(creds),
		config.WithHTTPClient(client),
	}

	if c.endpoint != "" && c.endpoint != "aws" {
		opts = append(opts,
			config.WithEndpointResolverWithOptions(c))
	}

	if c.checksumDisable {
		opts = append(opts,
			config.WithAPIOptions([]func(*middleware.Stack) error{v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware}))
	}

	if c.debug {
		opts = append(opts,
			config.WithClientLogMode(aws.LogSigning|aws.LogRetries|aws.LogRequest|aws.LogResponse|aws.LogRequestEventMessage|aws.LogResponseEventMessage))
	}

	cfg, err := config.LoadDefaultConfig(
		context.TODO(), opts...)
	if err != nil {
		log.Fatalln("error:", err)
	}

	return cfg
}

func (c *S3Conf) UploadData(r io.Reader, bucket, object string) error {
	uploader := manager.NewUploader(c.client)
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
	downloader := manager.NewDownloader(c.client)
	downloader.PartSize = c.partSize
	downloader.Concurrency = c.concurrency

	downinfo := &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &object,
	}

	return downloader.Download(context.Background(), w, downinfo)
}

func (c *S3Conf) DeleteObject(bucket, object string) error {
	_, err := c.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &object,
	})

	return err
}
