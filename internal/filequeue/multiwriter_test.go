package filequeue

import (
	"bytes"
	"io"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
)

func TestMultiWriteAtCloser_Close(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	errTest := errors.New("test error")

	m0 := NewMockWriteAtCloser(ctrl)
	m1 := NewMockWriteAtCloser(ctrl)
	m2 := NewMockWriteAtCloser(ctrl)
	mw := MultiWriteAtCloser{m0, m1, m2}

	gomock.InOrder(
		m0.EXPECT().Close().Return(nil).Times(1),
		m1.EXPECT().Close().Return(errTest).Times(1),
		m2.EXPECT().Close().Return(nil).Times(1),
	)

	err := mw.Close()
	if !errors.Is(err, errTest) {
		t.Error(err)
	}
}

func TestMultiWriteAtCloser_WriteAt(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	errTest := errors.New("test error")
	input := []byte("test input here")

	m0 := NewMockWriteAtCloser(ctrl)
	m1 := NewMockWriteAtCloser(ctrl)
	m2 := NewMockWriteAtCloser(ctrl)
	mw := MultiWriteAtCloser{m0, m1, m2}

	gomock.InOrder(
		m0.EXPECT().WriteAt(input, int64(123)).Return(len(input), nil).Times(1),
		m1.EXPECT().WriteAt(input, int64(123)).Return(len(input), nil).Times(1),
		m2.EXPECT().WriteAt(input, int64(123)).Return(len(input), nil).Times(1),
		m0.EXPECT().WriteAt(input, int64(456)).Return(len(input)-1, io.EOF).Times(1),
		m0.EXPECT().WriteAt(input, int64(789)).Return(0, errTest).Times(1),
	)

	err := mw.WriteAt(input, 123)
	if err != nil {
		t.Error(err)
	}

	err = mw.WriteAt(input, 456)
	if err == nil || err.Error() != "incomplete write" {
		t.Error(err)
	}

	err = mw.WriteAt(input, 789)
	if !errors.Is(err, errTest) {
		t.Error(err)
	}
}

func TestMultiWriteAtCloser_CopyNAt(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	input := []byte("test input here")
	errTest := errors.New("test error")

	m0 := NewMockWriteAtCloser(ctrl)
	m1 := NewMockWriteAtCloser(ctrl)
	m2 := NewMockWriteAtCloser(ctrl)
	mw := MultiWriteAtCloser{m0, m1, m2}

	gomock.InOrder(
		m0.EXPECT().WriteAt(input, int64(123)).Return(len(input), nil).Times(1),
		m1.EXPECT().WriteAt(input, int64(123)).Return(len(input), nil).Times(1),
		m2.EXPECT().WriteAt(input, int64(123)).Return(len(input), nil).Times(1),
		m0.EXPECT().WriteAt(input, int64(789)).Return(0, errTest).Times(1),
	)

	r := bytes.NewBuffer(nil)
	_, err := r.Write(input)
	if err != nil {
		t.Error(err)
	}

	err = mw.CopyNAt(r, int64(len(input)), 123)
	if err != nil {
		t.Error(err)
	}

	err = mw.CopyNAt(r, 32*1024+1, 456)
	if !errors.Is(err, io.EOF) {
		t.Error(err)
	}

	_, err = r.Write(input)
	if err != nil {
		t.Error(err)
	}
	err = mw.CopyNAt(r, int64(len(input)), 789)
	if !errors.Is(err, errTest) {
		t.Error(err)
	}
}
