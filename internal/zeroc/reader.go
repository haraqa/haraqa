package zeroc

import (
	"os"
)

func NewReader(f *os.File, offset int64, readLength int) *Reader {
	r := &Reader{
		file:   f,
		offset: offset,
		count:  readLength,
	}
	return r
}

type Reader struct {
	file   *os.File
	offset int64
	count  int
}
