// +build !linux

package zeroc

import (
	"io"
)

func (r *Reader) WriteTo(w io.Writer) (int64, error) {
	_, err := r.file.Seek(r.offset, io.SeekStart)
	if err != nil {
		return 0, err
	}

	return io.CopyN(w, r.file, int64(r.count))
}
