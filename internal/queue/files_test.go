package queue

import (
	"bytes"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/haraqa/haraqa/internal/mocks"
	"github.com/haraqa/haraqa/internal/zeroc"
	"github.com/pkg/errors"
)

func TestDatum(t *testing.T) {
	var d datum

	d.SetOffsetID(53).SetStartAt(87).SetMsgSize(93)

	if d.GetOffsetID() != 53 {
		t.Log(d.GetOffsetID())
		t.Fail()
	}

	if d.GetStartAt() != 87 {
		t.Log(d.GetStartAt())
		t.Fail()
	}

	if d.GetMsgSize() != 93 {
		t.Log(d.GetMsgSize())
		t.Fail()
	}
}

func TestWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	errMock := errors.New("mock error")

	mockWriter := mocks.NewMockReadWriteCloser(ctrl)

	gomock.InOrder(
		mockWriter.EXPECT().Write(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			var dat1, dat2 datum
			dat1.SetOffsetID(15).SetStartAt(31).SetMsgSize(47)
			dat2.SetOffsetID(16).SetStartAt(31 + 47).SetMsgSize(53)
			if !bytes.Equal(append(dat1[:], dat2[:]...), b) {
				return 0, errors.Errorf("unexpected byte write %v", b)
			}
			return len(b), nil
		}),
		mockWriter.EXPECT().Write(gomock.Any()).Return(0, errMock),
	)

	mw := &zeroc.MultiWriter{
		W: mockWriter,
	}

	err := writeDat(mw, 15, 31, []int64{47, 53})
	if err != nil {
		t.Fatal(err)
	}

	err = writeDat(mw, 15, 31, []int64{47, 53})
	if errors.Cause(err) != errMock {
		t.Fatal(err)
	}
}

func TestRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errMock := errors.New("mock error")
	f := mocks.NewMockFile(ctrl)
	info := mocks.NewMockFileInfo(ctrl)
	gomock.InOrder(
		// invalid stat
		f.EXPECT().Stat().Return(nil, errMock),
		f.EXPECT().Close().Return(nil),

		// empty file
		f.EXPECT().Stat().Return(info, nil),
		info.EXPECT().Size().Return(int64(datumLength-1)),
		f.EXPECT().Close().Return(nil),

		// bad read
		f.EXPECT().Stat().Return(info, nil),
		info.EXPECT().Size().Return(int64(datumLength*2)),
		f.EXPECT().ReadAt(gomock.Any(), int64(datumLength)).Return(0, errMock),
		f.EXPECT().Close().Return(nil),

		// empty read
		f.EXPECT().ReadAt(gomock.Any(), int64(3*datumLength)).Return(0, nil),
		f.EXPECT().Close().Return(nil),
	)

	// invalid stat
	_, _, err := readDat(f, -1, -1)
	if errors.Cause(err) != errMock {
		t.Fatal(err)
	}

	// empty file
	startAt, msgSizes, err := readDat(f, -1, -1)
	if err != nil {
		t.Fatal(err)
	}
	if startAt != 0 || len(msgSizes) != 0 {
		t.Fatal(startAt, msgSizes)
	}

	// bad read
	_, _, err = readDat(f, -1, -1)
	if errors.Cause(err) != errMock {
		t.Fatal(err)
	}

	// empty read
	startAt, msgSizes, err = readDat(f, 3, 10)
	if err != nil {
		t.Fatal(err)
	}
	if startAt != 0 || len(msgSizes) != 0 {
		t.Fatal(startAt, msgSizes)
	}
}
