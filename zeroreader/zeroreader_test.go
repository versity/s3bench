package zeroreader_test

import (
	"io"
	"testing"

	"github.com/versity/s3bench/zeroreader"
)

const (
	MB = 1048576
)

func TestZReader1(t *testing.T) {
	zr := zeroreader.New(100*MB, 1*MB)
	buf := make([]byte, 1*MB)
	var tot int
	for {
		n, err := zr.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read %v: %v", tot, err)
		}
		tot += n
	}
	if tot != 100*MB {
		t.Errorf("got %v, expected %v", tot, 100*MB)
	}
}

func TestZReader2(t *testing.T) {
	zr := zeroreader.New(100*MB, 1*MB)
	buf := make([]byte, 5*MB)
	var tot int
	for {
		n, err := zr.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read %v: %v", tot, err)
		}
		tot += n
	}
	if tot != 100*MB {
		t.Errorf("got %v, expected %v", tot, 100*MB)
	}
}

func TestZReader3(t *testing.T) {
	zr := zeroreader.New(100*MB, 5*MB)
	buf := make([]byte, 1*MB)
	var tot int
	for {
		n, err := zr.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read %v: %v", tot, err)
		}
		tot += n
	}
	if tot != 100*MB {
		t.Errorf("got %v, expected %v", tot, 100*MB)
	}
}
