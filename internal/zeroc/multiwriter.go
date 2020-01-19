package zeroc

import (
	"io"
	"os"

	"github.com/pkg/errors"
)

// MultiWriter uses splice and tee to perform a zero copy between a connection and
// one or more files
type MultiWriter struct {
	files  []*os.File
	w      io.Writer
	pipes  [][2]int
	limit  int64
	offset int64
}

// NewMultiWriter returns a new instance of a MultiWriter for the given files
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

// SetLimit sets the number of bytes the MultiWriter should pull from the connection
func (w *MultiWriter) SetLimit(n int64) *MultiWriter {
	w.limit = n
	return w
}

// IncreaseOffset increases the relative file offset to begin writing to in the files
func (w *MultiWriter) IncreaseOffset(n int64) *MultiWriter {
	w.offset += n
	return w
}

// Offset returns the current file offset
func (w *MultiWriter) Offset() int64 {
	return w.offset
}

// Close closes the multiwriter and all underlying files
func (w *MultiWriter) Close() error {
	for i := range w.files {
		w.files[i].Close()
	}

	return nil
}

// Write writes a byte slice to all files using the io.MultiWriter
func (w *MultiWriter) Write(b []byte) (int, error) {
	for i := range w.files {
		_, err := w.files[i].Seek(w.offset, io.SeekStart)
		if err != nil {
			return 0, err
		}
	}

	return w.w.Write(b)
}
