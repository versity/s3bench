package randreader

import (
	"crypto/rand"
	"io"
)

type RReader struct {
	buf      []byte
	dataleft int
}

func New(totalsize, bufsize int) *RReader {
	b := make([]byte, bufsize)
	rand.Read(b)
	return &RReader{buf: b, dataleft: totalsize}
}

func (r *RReader) Read(p []byte) (int, error) {
	n := min(len(p), len(r.buf), r.dataleft)
	r.dataleft -= n
	err := error(nil)
	if n == 0 {
		err = io.EOF
	}
	return copy(p, r.buf[:n]), err
}

func min(values ...int) int {
	if len(values) == 0 {
		return 0
	}

	min := values[0]
	for _, v := range values {
		if v < min {
			min = v
		}
	}

	return min
}
