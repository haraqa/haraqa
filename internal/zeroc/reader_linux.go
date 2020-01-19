package zeroc

import (
	"io"
	"syscall"

	"github.com/pkg/errors"
)

// WriteTo copies from the Reader to the writer starting at the offset given in
//  zeroc.NewReader
func (r *Reader) WriteTo(w io.Writer) (int64, error) {
	conn, ok := w.(fd)
	if !ok {
		return 0, errors.New("missing Fd method on writer input")
	}

	offset := r.offset
	count := r.count

	n, err := syscall.Sendfile(int(conn.Fd()), int(r.file.Fd()), &offset, count)
	return int64(n), err
}
