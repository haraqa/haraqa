package zeroc

import (
	"io"
	"os"

	"github.com/pkg/errors"
)

type MultiWriter struct {
	//sync.Mutex
	files  []*os.File
	w      io.Writer
	pipes  [][2]int
	limit  int64
	offset int64
}

func NewMultiWriter(files ...*os.File) (*MultiWriter, error) {
	w := make([]io.Writer, len(files))
	for i := range w {
		w[i] = files[i]
	}

	pipes, err := getPipes(len(w))
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new MultiWriter")
	}

	return &MultiWriter{
		files: files,
		w:     io.MultiWriter(w...),
		pipes: pipes,
		limit: 0,
	}, nil
}

func (w *MultiWriter) SetLimit(n int64) *MultiWriter {
	w.limit = n
	return w
}

func (w *MultiWriter) IncreaseOffset(n int64) *MultiWriter {
	w.offset += n
	return w
}

func (w *MultiWriter) Offset() int64 {
	return w.offset
}

func (w *MultiWriter) Close() error {
	for i := range w.files {
		w.files[i].Close()
	}
	return nil
}

func (w *MultiWriter) Write(b []byte) (int, error) {
	for i := range w.files {
		_, err := w.files[i].Seek(w.offset, os.SEEK_SET)
		if err != nil {
			return 0, err
		}
	}

	return w.w.Write(b)
}
