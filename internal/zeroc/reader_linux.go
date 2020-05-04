package zeroc

import (
	"io"
	"syscall"
)

// WriteTo copies from the Reader to the writer starting at the offset given in
//  zeroc.NewReader
func (r *Reader) WriteTo(w io.Writer) (int64, error) {
	conn, ok := w.(fd)
	if !ok {
		n, err := r.file.Seek(r.offset, io.SeekStart)
		if err != nil {
			return n, err
		}
		return io.CopyN(w, r.file, int64(r.count))
	}

	offset := r.offset
	count := r.count

	n, err := syscall.Sendfile(int(conn.Fd()), int(r.file.Fd()), &offset, count)
	return int64(n), err
}
