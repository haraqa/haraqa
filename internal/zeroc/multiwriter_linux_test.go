package zeroc

import (
	"io"
	"os"
	"syscall"
	"testing"

	"github.com/pkg/errors"
)

func TestPipeFailure(t *testing.T) {
	errMock := errors.New("mock error")
	syscallPipe = func([]int) error {
		return errMock
	}
	defer func() {
		syscallPipe = syscall.Pipe
	}()

	_, err := getPipes(123)
	if errors.Cause(err) != errMock {
		t.Fatal(err)
	}
}

type reader struct {
	io.Reader
}

func (r reader) Fd() uintptr {
	return 0
}

func TestTeeFailure(t *testing.T) {
	errMock := errors.New("mock error")
	syscallSplice = func(_ int, _ *int64, _ int, _ *int64, n int, _ int) (int64, error) {
		return int64(n - 1), nil
	}
	syscallTee = func(_, _, n, _ int) (int64, error) {
		return 0, errMock
	}
	defer func() {
		syscallTee = syscall.Tee
		syscallSplice = syscall.Splice
	}()

	w := MultiWriter{
		limit: 100,
		files: []*os.File{new(os.File), new(os.File)},
		pipes: [][2]int{{1, 2}, {3, 4}},
	}

	r := reader{}
	_, err := w.ReadFrom(r)
	if errors.Cause(err) != errMock {
		t.Fatal(err)
	}

	syscallTee = func(_, _, n, _ int) (int64, error) {
		return 1, nil
	}

	_, err = w.ReadFrom(r)
	if errors.Cause(err).Error() != "incomplete write to pipe" {
		t.Fatal(err)
	}
}

func TestSpliceFailure(t *testing.T) {
	syscallSplice = func(_ int, _ *int64, _ int, _ *int64, n int, _ int) (int64, error) {
		return int64(n - 1), nil
	}
	syscallTee = func(_, _, n, _ int) (int64, error) {
		return int64(n), nil
	}
	defer func() {
		syscallTee = syscall.Tee
		syscallSplice = syscall.Splice
	}()

	f, _ := os.Create("multiwrite.test")

	w := MultiWriter{
		limit: 100,
		files: []*os.File{f},
		pipes: [][2]int{{1, 2}},
	}

	r := reader{}
	_, err := w.ReadFrom(r)
	if errors.Cause(err).Error() != "incomplete write to file" {
		t.Fatal(err)
	}
}
