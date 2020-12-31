package filequeue

import (
	"io"

	"github.com/pkg/errors"
)

//go:generate go run github.com/golang/mock/mockgen -source multiwriter.go -package filequeue -destination multiwriter_mocks_test.go

// WriteAtCloser is a combination of io.Writer and io.Closer, similar to io.WriteCloser
type WriteAtCloser interface {
	io.Closer
	io.WriterAt
}

// MultiWriteAtCloser provides methods for a slice of WriteAtCloser
type MultiWriteAtCloser []WriteAtCloser

// Close closes all writers in the slice
func (mw MultiWriteAtCloser) Close() error {
	var err error
	for _, w := range mw {
		if e := w.Close(); e != nil {
			err = e
		}
	}
	return err
}

// WriteAt performs a WriteAt to each of the writers in order
func (mw MultiWriteAtCloser) WriteAt(p []byte, off int64) error {
	for _, w := range mw {
		n, err := w.WriteAt(p, off)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if n != len(p) {
			return errors.New("incomplete write")
		}
	}
	return nil
}

// CopyNAt performs a CopyNAt to each of the writers in order
func (mw MultiWriteAtCloser) CopyNAt(r io.Reader, N, off int64) error {
	// get log buffer
	buf := bufPool.Get().([]byte)
	if N > int64(cap(buf)) {
		buf = make([]byte, N)
	} else {
		buf = buf[:N]
	}
	defer bufPool.Put(buf)

	// read to buffer
	_, err := io.ReadAtLeast(r, buf, len(buf))
	if err != nil {
		return errors.Wrap(err, "unable to read input")
	}

	// write buffer to logs
	err = mw.WriteAt(buf, off)
	if err != nil {
		return errors.Wrap(err, "unable to copy to log file")
	}
	return nil
}
