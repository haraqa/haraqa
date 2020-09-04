package filequeue

import (
	"io"

	"github.com/pkg/errors"
)

type MultiWriteAtCloser []interface {
	io.Closer
	io.WriterAt
}

func (mw MultiWriteAtCloser) Close() error {
	var err error
	for _, w := range mw {
		if e := w.Close(); e != nil {
			err = e
		}
	}
	return err
}

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
