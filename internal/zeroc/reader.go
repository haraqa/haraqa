package zeroc

import (
	"os"
)

// NewReader creates a new zero copy reader which can WriteTo a connection
func NewReader(f *os.File, offset int64, readLength int) *Reader {
	r := &Reader{
		file:   f,
		offset: offset,
		count:  readLength,
	}
	return r
}

// Reader is a reader which uses the sendfile syscall on unix or the io.CopyN
// method on non-linux systems
type Reader struct {
	file   *os.File
	offset int64
	count  int
}
