package protocol_test

import (
	"bytes"
	"encoding/binary"
	fmt "fmt"
	io "io"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/haraqa/haraqa/internal/mocks"
	"github.com/haraqa/haraqa/internal/protocol"
	"github.com/pkg/errors"
)

func TestErrorToResponse(t *testing.T) {
	err := errors.New("some error")
	r, w := io.Pipe()

	go protocol.ErrorToResponse(w, err)

	b := make([]byte, 6+len(err.Error()))
	n, e := r.Read(b[:])
	if e != nil {
		t.Fatal(e)
	}
	if n != len(b) {
		t.Fatal(n, len(b))
	}

	if b[0] != 0 || b[1] != protocol.TypeError {
		t.Fatal(b)
	}

	l := binary.BigEndian.Uint32(b[2:6])
	if int(l) != len(err.Error()) {
		t.Fatal(l, len(b))
	}

	if string(b[6:]) != err.Error() {
		t.Fatal(string(b[6:]))
	}
}

func TestExtendBuffer(t *testing.T) {
	var buf []byte
	protocol.ExtendBuffer(&buf, 23)
	if len(buf) != 23 {
		t.Fatal(len(buf))
	}
	protocol.ExtendBuffer(&buf, 17)
	if len(buf) != 17 {
		t.Fatal(len(buf))
	}
	protocol.ExtendBuffer(&buf, 25)
	if len(buf) != 25 {
		t.Fatal(len(buf))
	}
}

func TestPing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	t.Run("Valid write", func(t *testing.T) {
		rw := mocks.NewMockReadWriteCloser(ctrl)
		rw.EXPECT().Write([]byte{0, protocol.TypePing, 0, 0, 0, 0}).Return(6, nil)
		rw.EXPECT().Read([]byte{0, protocol.TypePing, 0, 0, 0, 0}).Return(6, nil)
		err := protocol.Ping(rw)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("Valid write", func(t *testing.T) {
		mockErr := errors.New("mock error")
		rw := mocks.NewMockReadWriteCloser(ctrl)
		rw.EXPECT().Write([]byte{0, protocol.TypePing, 0, 0, 0, 0}).Return(0, mockErr)
		err := protocol.Ping(rw)
		if err != mockErr {
			t.Fatal(err)
		}
	})
}

func TestClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	wc := mocks.NewMockReadWriteCloser(ctrl)
	wc.EXPECT().Write([]byte{0, protocol.TypeClose, 0, 0, 0, 0}).Return(6, nil)
	wc.EXPECT().Close().Return(nil)
	err := protocol.Close(wc)
	if err != nil {
		t.Fatal(err)
	}
}

func TestProduceRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockErr := errors.New("mock error")

	t.Run("invalid reads", func(t *testing.T) {
		req := protocol.ProduceRequest{}
		err := req.Read(nil)
		if err.Error() != "invalid produce header length" {
			t.Fatal(err)
		}
		err = req.Read([]byte{0, 2})
		if err.Error() != "invalid topic length" {
			t.Fatal(err)
		}
		err = req.Read([]byte{0, 2, 90, 90, 0})
		if err.Error() != "invalid messages length" {
			t.Fatal(err)
		}
	})

	t.Run("write/read", func(t *testing.T) {
		writeReq := protocol.ProduceRequest{
			Topic:    []byte("write-topic"),
			MsgSizes: []int64{3, 5},
		}
		w := mocks.NewMockReadWriteCloser(ctrl)
		w.EXPECT().Write(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			if b[0] != 0 || b[1] != protocol.TypeProduce {
				t.Fatal(b)
			}
			l := binary.BigEndian.Uint32(b[2:6])
			if int(l) != len(b)-6 {
				t.Fatal(l)
			}
			readReq := protocol.ProduceRequest{}
			err := readReq.Read(b[6:])
			if err != nil {
				return 0, err
			}
			if !bytes.Equal(writeReq.Topic, readReq.Topic) {
				t.Fatal(string(readReq.Topic))
			}
			if len(writeReq.MsgSizes) != len(readReq.MsgSizes) {
				t.Fatal(len(readReq.MsgSizes))
			}
			if writeReq.MsgSizes[0] != readReq.MsgSizes[0] || writeReq.MsgSizes[1] != readReq.MsgSizes[1] {
				t.Fatal(readReq.MsgSizes)
			}
			return 0, mockErr
		})
		err := writeReq.Write(w)
		if err != mockErr {
			t.Fatal(err)
		}
	})
}

func TestConsumeRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockErr := errors.New("mock error")

	t.Run("invalid reads", func(t *testing.T) {
		req := protocol.ConsumeRequest{}
		err := req.Read(nil)
		if err.Error() != "invalid consume header length" {
			t.Fatal(err)
		}
		err = req.Read([]byte{0, 2})
		if err.Error() != "invalid topic length" {
			t.Fatal(err)
		}
	})

	t.Run("write/read", func(t *testing.T) {
		writeReq := protocol.ConsumeRequest{
			Topic:  []byte("write-topic"),
			Offset: 3,
			Limit:  10,
		}
		w := mocks.NewMockReadWriteCloser(ctrl)
		w.EXPECT().Write(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			if b[0] != 0 || b[1] != protocol.TypeConsume {
				t.Fatal(b)
			}
			l := binary.BigEndian.Uint32(b[2:6])
			if int(l) != len(b)-6 {
				t.Fatal(l)
			}
			readReq := protocol.ConsumeRequest{}
			err := readReq.Read(b[6:])
			if err != nil {
				return 0, err
			}
			if !bytes.Equal(writeReq.Topic, readReq.Topic) {
				t.Fatal(string(readReq.Topic))
			}
			if writeReq.Limit != readReq.Limit {
				t.Fatal(readReq.Limit)
			}
			if writeReq.Offset != readReq.Offset {
				t.Fatal(readReq.Offset)
			}
			return 0, mockErr
		})
		err := writeReq.Write(w)
		if err != mockErr {
			t.Fatal(err)
		}
	})
}

func TestConsumeResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockErr := errors.New("mock error")

	t.Run("invalid reads", func(t *testing.T) {
		req := protocol.ConsumeResponse{}
		err := req.Read([]byte{0, 2})
		if err.Error() != "invalid messages length" {
			t.Fatal(err)
		}
	})

	t.Run("write/read", func(t *testing.T) {
		writeReq := protocol.ConsumeResponse{
			MsgSizes: []int64{3, 5},
		}
		w := mocks.NewMockReadWriteCloser(ctrl)
		w.EXPECT().Write(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			if b[0] != 0 || b[1] != protocol.TypeConsume {
				t.Fatal(b)
			}
			l := binary.BigEndian.Uint32(b[2:6])
			if int(l) != len(b)-6 {
				t.Fatal(l)
			}
			readReq := protocol.ConsumeResponse{}
			err := readReq.Read(b[6:])
			if err != nil {
				return 0, err
			}
			if len(writeReq.MsgSizes) != len(readReq.MsgSizes) {
				t.Fatal(len(readReq.MsgSizes))
			}
			if writeReq.MsgSizes[0] != readReq.MsgSizes[0] || writeReq.MsgSizes[1] != readReq.MsgSizes[1] {
				t.Fatal(readReq.MsgSizes)
			}
			return 0, mockErr
		})
		err := writeReq.Write(w)
		if err != mockErr {
			t.Fatal(err)
		}
	})
}

func TestReadPrefix(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockErr := errors.New("mock error")

	t.Run("With bad prefix", func(t *testing.T) {
		_, _, err := protocol.ReadPrefix(nil, []byte{0, 0, 0, 0})
		if err.Error() != "invalid prefix length" {
			t.Fatal(err)
		}

		prefix1 := [6]byte{}
		prefix2 := [6]byte{1}
		prefix3 := [6]byte{0, 87}
		r := mocks.NewMockReadWriteCloser(ctrl)

		r.EXPECT().Read(prefix1[:]).Return(0, mockErr)
		_, _, err = protocol.ReadPrefix(r, prefix1[:])
		if errors.Cause(err) != mockErr {
			t.Fatal(err)
		}

		r.EXPECT().Read(prefix2[:]).Return(6, nil)
		_, _, err = protocol.ReadPrefix(r, prefix2[:])
		if err.Error() != fmt.Sprintf("invalid type %v", prefix2) {
			t.Fatal(err)
		}

		r.EXPECT().Read(prefix3[:]).Return(6, nil)
		_, _, err = protocol.ReadPrefix(r, prefix3[:])
		if err.Error() != fmt.Sprintf("invalid type %v", prefix3) {
			t.Fatal(err)
		}
	})

	t.Run("with valid types", func(t *testing.T) {
		r := mocks.NewMockReadWriteCloser(ctrl)
		r.EXPECT().Read(gomock.Any()).Return(6, nil).AnyTimes()

		prefix := [6]byte{0, protocol.TypePing, 0, 0, 0, 255}
		typ, hLen, err := protocol.ReadPrefix(r, prefix[:])
		if err != nil {
			t.Fatal(err)
		}
		if hLen != 0 {
			t.Fatal(hLen)
		}
		if typ != protocol.TypePing {
			t.Fatal(t)
		}

		prefix = [6]byte{0, protocol.TypeClose, 0, 0, 0, 255}
		typ, hLen, err = protocol.ReadPrefix(r, prefix[:])
		if err != nil {
			t.Fatal(err)
		}
		if hLen != 0 {
			t.Fatal(hLen)
		}
		if typ != protocol.TypeClose {
			t.Fatal(t)
		}

		prefix = [6]byte{0, protocol.TypeProduce, 0, 0, 0, 255}
		typ, hLen, err = protocol.ReadPrefix(r, prefix[:])
		if err != nil {
			t.Fatal(err)
		}
		if hLen != 255 {
			t.Fatal(hLen)
		}
		if typ != protocol.TypeProduce {
			t.Fatal(t)
		}

		prefix = [6]byte{0, protocol.TypeConsume, 0, 0, 0, 255}
		typ, hLen, err = protocol.ReadPrefix(r, prefix[:])
		if err != nil {
			t.Fatal(err)
		}
		if hLen != 255 {
			t.Fatal(hLen)
		}
		if typ != protocol.TypeConsume {
			t.Fatal(t)
		}
	})
	t.Run("with error type", func(t *testing.T) {
		r := mocks.NewMockReadWriteCloser(ctrl)
		gomock.InOrder(
			r.EXPECT().Read(gomock.Any()).Return(6, nil),
			r.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				n := copy(b, []byte(protocol.ErrTopicExists.Error()))
				return n, nil
			}),
			r.EXPECT().Read(gomock.Any()).Return(6, nil),
			r.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				n := copy(b, []byte(protocol.ErrTopicDoesNotExist.Error()))
				return n, nil
			}),
			r.EXPECT().Read(gomock.Any()).Return(6, nil),
			r.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				n := copy(b, []byte(mockErr.Error()))
				return n, nil
			}),
		)
		prefix := [6]byte{0, protocol.TypeError}

		// err topic exists
		binary.BigEndian.PutUint32(prefix[2:], uint32(len(protocol.ErrTopicExists.Error())))
		typ, hLen, err := protocol.ReadPrefix(r, prefix[:])
		if err != protocol.ErrTopicExists {
			t.Fatal(err)
		}
		if hLen != uint32(len(protocol.ErrTopicExists.Error())) {
			t.Fatal(hLen)
		}
		if typ != protocol.TypeError {
			t.Fatal(t)
		}

		// err topic does not exist
		binary.BigEndian.PutUint32(prefix[2:], uint32(len(protocol.ErrTopicDoesNotExist.Error())))
		typ, hLen, err = protocol.ReadPrefix(r, prefix[:])
		if err != protocol.ErrTopicDoesNotExist {
			t.Fatal(err)
		}
		if hLen != uint32(len(protocol.ErrTopicDoesNotExist.Error())) {
			t.Fatal(hLen)
		}
		if typ != protocol.TypeError {
			t.Fatal(t)
		}

		// custom error
		binary.BigEndian.PutUint32(prefix[2:], uint32(len(mockErr.Error())))
		typ, hLen, err = protocol.ReadPrefix(r, prefix[:])
		if err.Error() != mockErr.Error() {
			t.Fatal(err)
		}
		if hLen != uint32(len(mockErr.Error())) {
			t.Fatal(hLen)
		}
		if typ != protocol.TypeError {
			t.Fatal(t)
		}
	})
}
