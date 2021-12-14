package main

import (
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"time"

	"github.com/versity/s3bench/nullwriter"
	"github.com/versity/s3bench/randreader"
	"github.com/versity/s3bench/s3io"
	"github.com/versity/s3bench/zeroreader"
)

var (
	files           int
	concurrency     int
	chunksize       int64
	objectsize      int64
	awsID           string
	awsSecret       string
	awsRegion       string
	endpoint        string
	bucket          string
	prefix          string
	checksumDisable bool
	pathStyle       bool
	upload          bool
	download        bool
	rand            bool
)

func init() {
	flag.IntVar(&files, "n", 1, "number of objects to read/write")
	flag.IntVar(&concurrency, "concurrency", 1, "upload/download threads per object")
	flag.Int64Var(&chunksize, "chunksize", 64*1024*1024, "upload/download size per thread")
	flag.Int64Var(&objectsize, "objectsize", 0, "upload object size")
	flag.StringVar(&awsID, "access", "", "access key, or specify in AWS_ACCESS_KEY_ID env")
	flag.StringVar(&awsSecret, "secret", "", "secret key, or specify in AWS_SECRET_ACCESS_KEY env")
	flag.StringVar(&awsRegion, "region", "us-east-1", "bucket region")
	flag.StringVar(&endpoint, "endpoint", "", "s3 server endpoint, default aws")
	flag.StringVar(&bucket, "bucket", "", "s3 bucket")
	flag.StringVar(&prefix, "prefix", "", "object name prefix")
	flag.BoolVar(&checksumDisable, "disablechecksum", false, "disable server checksums")
	flag.BoolVar(&pathStyle, "pathstyle", false, "use pathstyle bucket addressing")
	flag.BoolVar(&upload, "upload", false, "upload data to s3")
	flag.BoolVar(&download, "download", false, "download data from s3")
	flag.BoolVar(&rand, "rand", false, "use random data (default is all 0s)")
}

func errorf(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
	os.Exit(2)
}

type result struct {
	elapsed time.Duration
	size    int64
	err     error
}

func main() {
	flag.Parse()

	if upload && download {
		errorf("must only specify one of upload or download")
	}
	if !upload && !download {
		errorf("must specify one of upload or download")
	}

	if bucket == "" {
		errorf("must specify bucket")
	}

	opts := []s3io.Option{
		s3io.WithAccess(awsID),
		s3io.WithSecret(awsSecret),
		s3io.WithRegion(awsRegion),
		s3io.WithEndpoint(endpoint),
		s3io.WithPartSize(chunksize),
		s3io.WithConcurrency(concurrency),
	}
	if checksumDisable {
		opts = append(opts, s3io.WithDisableChecksum())
	}
	if pathStyle {
		opts = append(opts, s3io.WithPathStyle())
	}

	s3conf := s3io.New(opts...)

	var sg sync.WaitGroup
	results := make([]result, files)
	start := time.Now()
	if upload {
		if objectsize == 0 {
			errorf("must specify object size for upload")
		}

		if objectsize > (10000 * chunksize) {
			errorf("object size can not exceed 10000 * chunksize")
		}

		for i := 0; i < files; i++ {
			sg.Add(1)
			go func(i int) {
				var r io.Reader
				if rand {
					r = randreader.New(int(objectsize), int(chunksize))
				} else {
					r = zeroreader.New(int(objectsize), int(chunksize))
				}

				start := time.Now()
				err := s3conf.UploadData(r, bucket, fmt.Sprintf("%v%v", prefix, i))
				results[i].elapsed = time.Since(start)
				results[i].err = err
				results[i].size = objectsize
				sg.Done()
			}(i)
		}
	}
	if download {
		for i := 0; i < files; i++ {
			sg.Add(1)
			go func(i int) {
				nw := nullwriter.New()
				start := time.Now()
				n, err := s3conf.DownloadData(nw, bucket, fmt.Sprintf("%v%v", prefix, i))
				results[i].elapsed = time.Since(start)
				results[i].err = err
				results[i].size = n
				sg.Done()
			}(i)
		}
	}
	sg.Wait()
	elapsed := time.Since(start)

	var tot int64
	for i, res := range results {
		if res.err != nil {
			fmt.Printf("%v: %v\n", i, res.err)
			continue
		}
		tot += res.size
		fmt.Printf("%v: %v in %v (%v MB/s)\n",
			i, res.size, res.elapsed,
			int(math.Ceil(float64(res.size)/res.elapsed.Seconds())/1048576))
	}

	fmt.Println()
	fmt.Printf("run perf: %v in %v (%v MB/s)\n",
		tot, elapsed, int(math.Ceil(float64(tot)/elapsed.Seconds())/1048576))
}
