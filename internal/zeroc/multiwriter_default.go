// +build !linux

package zeroc

import (
	"io"
)

func getPipes(n int) ([][2]int, error) {
	return nil, nil
}

// ReadFrom uses the io.CopyN method to copy data to an io.MultiWriter. See
//  multiwriter_linux.go for the linux implementation
func (w *MultiWriter) ReadFrom(r io.Reader) (int64, error) {

	for i := range w.files {
		_, err := w.files[i].Seek(w.offset, io.SeekStart)
		if err != nil {
			return 0, err
		}
	}

	return io.CopyN(w.w, r, w.limit)
}
