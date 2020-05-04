// +build !linux

package zeroc

import (
	"io"
)

// WriteTo copies from the Reader to the writer starting at the offset given in
//  zeroc.NewReader. See reader_linux.go for the linux implementation
func (r *Reader) WriteTo(w io.Writer) (int64, error) {
	_, ok := w.(fd)
	if !ok {
		n, err := r.file.Seek(r.offset, io.SeekStart)
		if err != nil {
			return n, err
		}
		return io.CopyN(w, r.file, int64(r.count))
	}

	_, err := r.file.Seek(r.offset, io.SeekStart)
	if err != nil {
		return 0, err
	}

	return io.CopyN(w, r.file, int64(r.count))
}
