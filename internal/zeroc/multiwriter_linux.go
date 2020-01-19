package zeroc

import (
	"io"
	"syscall"

	"github.com/pkg/errors"
)

const (
	spliceFlags int = 0x1 + 0x4 //unix.SPLICE_F_MOVE+unix.SPLICE_F_MORE
	readPipe        = 0
	writePipe       = 1
)

func getPipes(n int) ([][2]int, error) {
	pipes := make([][2]int, n)
	for i := range pipes {
		if err := syscall.Pipe(pipes[i][:]); err != nil {
			return nil, errors.Wrapf(err, "new pipe syscall failed")
		}
	}
	return pipes, nil
}

type fd interface {
	Fd() uintptr
}

// ReadFrom tees the input from a io.reader with file descriptor function Fd()
//  to a series of pipes and splices those to the underlying files to be written to
func (w *MultiWriter) ReadFrom(r io.Reader) (int64, error) {
	f, ok := r.(fd)
	if !ok {
		return 0, errors.New("missing Fd method on reader input")
	}

	var n int64
	for w.limit > 0 {
		// splice from source to first write pipe
		n1, err := syscall.Splice(int(f.Fd()), nil, w.pipes[0][writePipe], nil, int(w.limit), spliceFlags)
		if err != nil {
			return n, errors.Wrapf(err, "splice src syscall failed")
		}

		// tee first read pipe into every subsequent write pipe
		for i := range w.files[1:] {
			nx, err := syscall.Tee(w.pipes[0][readPipe], w.pipes[i+1][writePipe], int(n1), 0)
			if err != nil {
				return n, errors.Wrap(err, "tee syscall failed")
			}
			if nx != n1 {
				return n, errors.New("incomplete write to pipe")
			}
		}

		// splice from each readpipe to the respective dst file
		for i := range w.files {
			off := w.offset + n
			nx, err := syscall.Splice(w.pipes[i][readPipe], nil, int(w.files[i].Fd()), &off, int(n1), spliceFlags)
			if err != nil {
				return n, errors.Wrap(err, "splice dst syscall failed")
			}
			if nx != n1 {
				return n, errors.New("incomplete write to file")
			}
		}
		n += n1
		w.limit -= n1
	}
	return n, nil
}
