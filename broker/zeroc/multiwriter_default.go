// +build !linux

package zeroc

import (
	"io"
	"os"
)

func getPipes(n int) ([][2]int, error) {
	return nil, nil
}

func (w *MultiWriter) ReadFrom(r io.Reader) (int64, error) {

	for i := range w.files {
		_, err := w.files[i].Seek(w.offset, os.SEEK_SET)
		if err != nil {
			return 0, err
		}
	}

	return io.CopyN(w.w, r, int64(w.limit))
}
