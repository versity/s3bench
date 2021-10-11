package s3io

import (
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
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

func (c *S3Conf) getCreds() *credentials.Credentials {
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

	return credentials.NewStaticCredentials(c.awsID, c.awsSecret, "")
}

func (c *S3Conf) config() *aws.Config {
	creds := c.getCreds()

	config := aws.NewConfig().WithRegion(c.awsRegion).WithCredentials(creds)
	config = config.WithDisableSSL(c.disableSSL)
	config = config.WithDisableComputeChecksums(c.checksumDisable)
	config = config.WithS3ForcePathStyle(c.pathStyle)
	if c.endpoint != "" {
		config = config.WithEndpoint(c.endpoint)
	}

	return config
}

func (c *S3Conf) UploadData(r io.Reader, bucket, object string) error {
	uploader := s3manager.NewUploader(session.New(c.config()))
	uploader.PartSize = c.partSize
	uploader.Concurrency = c.concurrency

	upinfo := &s3manager.UploadInput{
		Body:   r,
		Bucket: &bucket,
		Key:    &object,
	}

	_, err := uploader.Upload(upinfo)
	return err
}

func (c *S3Conf) DownloadData(w io.WriterAt, bucket, object string) (int64, error) {
	downloader := s3manager.NewDownloader(session.New(c.config()))
	downloader.PartSize = c.partSize
	downloader.Concurrency = c.concurrency

	downinfo := &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &object,
	}

	return downloader.Download(w, downinfo)
}
