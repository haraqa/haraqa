package broker

import (
	"bytes"
	"io"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/haraqa/haraqa/internal/mocks"
	"github.com/haraqa/haraqa/internal/protocol"
	"github.com/pkg/errors"
)

func TestDataServer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var errMock = errors.New("mock error")
	b := &Broker{}

	t.Run("read eof error", func(t *testing.T) {
		conn := mocks.NewMockReadWriteCloser(ctrl)
		gomock.InOrder(
			conn.EXPECT().Read(gomock.Any()).Return(0, io.EOF),
			conn.EXPECT().Close().Return(nil),
		)
		b.handleDataConn(conn)
	})

	t.Run("read error", func(t *testing.T) {
		conn := mocks.NewMockReadWriteCloser(ctrl)
		gomock.InOrder(
			conn.EXPECT().Read(gomock.Any()).Return(0, errMock),
			conn.EXPECT().Write(gomock.Any()).Return(0, nil),
			conn.EXPECT().Close().Return(nil),
		)
		b.handleDataConn(conn)
	})

	t.Run("read success, close prefix", func(t *testing.T) {
		conn := mocks.NewMockReadWriteCloser(ctrl)
		gomock.InOrder(
			conn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				return copy(b, []byte{0, protocol.TypeClose, 0, 0, 0, 0}), nil
			}),
			conn.EXPECT().Close().Return(nil),
		)
		b.handleDataConn(conn)
	})

	t.Run("read success, ping", func(t *testing.T) {
		conn := mocks.NewMockReadWriteCloser(ctrl)
		gomock.InOrder(
			conn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				return copy(b, []byte{0, protocol.TypePing, 0, 0, 0, 0}), nil
			}),
			conn.EXPECT().Write(gomock.Any()).Return(0, errMock),
			conn.EXPECT().Write(gomock.Any()).Return(0, nil),
			conn.EXPECT().Close().Return(nil),
		)
		b.handleDataConn(conn)
	})

	t.Run("read success, produce", func(t *testing.T) {
		conn := mocks.NewMockReadWriteCloser(ctrl)
		gomock.InOrder(
			conn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				return copy(b, []byte{0, protocol.TypeProduce, 0, 0, 0, 0}), nil
			}),
			conn.EXPECT().Write(gomock.Any()).Return(0, nil),
			conn.EXPECT().Close().Return(nil),
		)
		b.handleDataConn(conn)
	})
	t.Run("read success, consume", func(t *testing.T) {
		conn := mocks.NewMockReadWriteCloser(ctrl)
		gomock.InOrder(
			conn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				return copy(b, []byte{0, protocol.TypeConsume, 0, 0, 0, 0}), nil
			}),
			conn.EXPECT().Write(gomock.Any()).Return(0, nil),
			conn.EXPECT().Close().Return(nil),
		)
		b.handleDataConn(conn)
	})
}

func TestProduceHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var (
		errMock = errors.New("mock error")
		b       = &Broker{
			MaxSize: 200,
			M:       noopMetrics{},
		}
		produceReq = &protocol.ProduceRequest{}
		buf        []byte
		prefix     [6]byte
	)

	t.Run("read error", func(t *testing.T) {
		conn := mocks.NewMockReadWriteCloser(ctrl)
		gomock.InOrder(
			conn.EXPECT().Read(gomock.Any()).Return(0, errMock),
			conn.EXPECT().Write(gomock.Any()).Return(0, nil),
		)
		err := b.handleProduce(conn, produceReq, &buf, 5, prefix[:])
		if errors.Cause(err) != errMock {
			t.Fatal(err)
		}
	})
	t.Run("read success, invalid message", func(t *testing.T) {
		conn := mocks.NewMockReadWriteCloser(ctrl)
		gomock.InOrder(
			conn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				return copy(b, []byte{0, 0, 0, 0, 0}), nil
			}),
			conn.EXPECT().Write(gomock.Any()).Return(0, nil),
		)
		err := b.handleProduce(conn, produceReq, &buf, 5, prefix[:])
		if errors.Cause(err).Error() != "invalid messages length" {
			t.Fatal(err)
		}
	})

	// serialize a valid message
	buffer := new(bytes.Buffer)
	err := (&protocol.ProduceRequest{
		Topic:    []byte("produce-topic"),
		MsgSizes: []int64{5, 6},
	}).Write(buffer)
	if err != nil {
		t.Fatal(err)
	}
	msg := buffer.Bytes()[6:]

	t.Run("read success, valid message, queue error", func(t *testing.T) {
		mockQ := mocks.NewMockQueue(ctrl)
		b.Q = mockQ
		conn := mocks.NewMockReadWriteCloser(ctrl)
		gomock.InOrder(
			conn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				return copy(b, msg), nil
			}),
			mockQ.EXPECT().Produce(gomock.Any(), gomock.Any(), gomock.Any()).Return(errMock),
			conn.EXPECT().Write(gomock.Any()).Return(0, nil),
		)
		err := b.handleProduce(conn, produceReq, &buf, uint32(len(msg)), prefix[:])
		if errors.Cause(err) != errMock {
			t.Fatal(err)
		}
	})

	t.Run("read success, valid message, queue success, write error", func(t *testing.T) {
		mockQ := mocks.NewMockQueue(ctrl)
		b.Q = mockQ
		conn := mocks.NewMockReadWriteCloser(ctrl)
		gomock.InOrder(
			conn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				return copy(b, msg), nil
			}),
			mockQ.EXPECT().Produce(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil),
			conn.EXPECT().Write(gomock.Any()).Return(0, errMock).Times(2),
		)
		err := b.handleProduce(conn, produceReq, &buf, uint32(len(msg)), prefix[:])
		if errors.Cause(err) != errMock {
			t.Fatal(err)
		}
	})

	t.Run("read success, valid message, queue success, write success", func(t *testing.T) {
		mockQ := mocks.NewMockQueue(ctrl)
		b.Q = mockQ
		conn := mocks.NewMockReadWriteCloser(ctrl)
		gomock.InOrder(
			conn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				return copy(b, msg), nil
			}),
			mockQ.EXPECT().Produce(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil),
			conn.EXPECT().Write(gomock.Any()).Return(6, nil),
		)
		err := b.handleProduce(conn, produceReq, &buf, uint32(len(msg)), prefix[:])
		if errors.Cause(err) != nil {
			t.Fatal(err)
		}
	})

	t.Run("read success, message too largs", func(t *testing.T) {
		b.MaxSize = 3
		conn := mocks.NewMockReadWriteCloser(ctrl)
		gomock.InOrder(
			conn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				return copy(b, msg), nil
			}),
			conn.EXPECT().Write(gomock.Any()).Return(0, nil),
		)
		err := b.handleProduce(conn, produceReq, &buf, uint32(len(msg)), prefix[:])
		if errors.Cause(err).Error() != "invalid message size. exceeds maximum limit of 3 bytes" {
			t.Fatal(err)
		}
	})
}

func TestConsumeHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var (
		errMock     = errors.New("mock error")
		b           = &Broker{M: noopMetrics{}}
		consumeReq  = &protocol.ConsumeRequest{}
		consumeResp = &protocol.ConsumeResponse{}
		buf         []byte
		prefix      [6]byte
	)

	t.Run("read error", func(t *testing.T) {
		conn := mocks.NewMockReadWriteCloser(ctrl)
		gomock.InOrder(
			conn.EXPECT().Read(gomock.Any()).Return(0, errMock),
			conn.EXPECT().Write(gomock.Any()).Return(0, nil),
		)
		err := b.handleConsume(conn, consumeReq, consumeResp, &buf, 5, prefix[:])
		if errors.Cause(err) != errMock {
			t.Fatal(err)
		}
	})
	t.Run("read success, invalid message", func(t *testing.T) {
		conn := mocks.NewMockReadWriteCloser(ctrl)
		gomock.InOrder(
			conn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				return copy(b, []byte{0, 0, 0, 0, 0}), nil
			}),
			conn.EXPECT().Write(gomock.Any()).Return(0, nil),
		)
		err := b.handleConsume(conn, consumeReq, consumeResp, &buf, 5, prefix[:])
		if errors.Cause(err).Error() != "invalid topic length" {
			t.Fatal(err)
		}
	})

	// serialize a valid message
	buffer := new(bytes.Buffer)
	err := (&protocol.ConsumeRequest{
		Topic:  []byte("produce-topic"),
		Offset: 0,
		Limit:  2,
	}).Write(buffer)
	if err != nil {
		t.Fatal(err)
	}
	msg := buffer.Bytes()[6:]

	t.Run("read success, valid message, queue error", func(t *testing.T) {
		mockQ := mocks.NewMockQueue(ctrl)
		b.Q = mockQ
		conn := mocks.NewMockReadWriteCloser(ctrl)
		gomock.InOrder(
			conn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				return copy(b, msg), nil
			}),
			mockQ.EXPECT().ConsumeInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, int64(0), nil, errMock),
			conn.EXPECT().Write(gomock.Any()).Return(0, nil),
		)
		err := b.handleConsume(conn, consumeReq, consumeResp, &buf, uint32(len(msg)), prefix[:])
		if errors.Cause(err) != errMock {
			t.Fatal(err)
		}
	})
	t.Run("read success, valid message, queue success, write 1 failure", func(t *testing.T) {
		mockQ := mocks.NewMockQueue(ctrl)
		b.Q = mockQ
		conn := mocks.NewMockReadWriteCloser(ctrl)
		gomock.InOrder(
			conn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				return copy(b, msg), nil
			}),
			mockQ.EXPECT().ConsumeInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, int64(0), nil, nil),
			conn.EXPECT().Write(gomock.Any()).Return(0, errMock),
			conn.EXPECT().Write(gomock.Any()).Return(0, nil),
		)
		err := b.handleConsume(conn, consumeReq, consumeResp, &buf, uint32(len(msg)), prefix[:])
		if errors.Cause(err) != errMock {
			t.Fatal(err)
		}
	})

	t.Run("read success, valid message, queue success, write 1 success, queue failure", func(t *testing.T) {
		mockQ := mocks.NewMockQueue(ctrl)
		b.Q = mockQ
		conn := mocks.NewMockReadWriteCloser(ctrl)
		gomock.InOrder(
			conn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				return copy(b, msg), nil
			}),
			mockQ.EXPECT().ConsumeInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, int64(0), []int64{0}, nil),
			conn.EXPECT().Write(gomock.Any()).Return(0, nil),
			mockQ.EXPECT().Consume(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errMock),
		)
		err := b.handleConsume(conn, consumeReq, consumeResp, &buf, uint32(len(msg)), prefix[:])
		if errors.Cause(err) != errMock {
			t.Fatal(err)
		}
	})

	t.Run("read success, valid message, queue success, write 1 success, queue success", func(t *testing.T) {
		mockQ := mocks.NewMockQueue(ctrl)
		b.Q = mockQ
		conn := mocks.NewMockReadWriteCloser(ctrl)
		gomock.InOrder(
			conn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
				return copy(b, msg), nil
			}),
			mockQ.EXPECT().ConsumeInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, int64(0), []int64{0}, nil),
			conn.EXPECT().Write(gomock.Any()).Return(0, nil),
			mockQ.EXPECT().Consume(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil),
		)
		err := b.handleConsume(conn, consumeReq, consumeResp, &buf, uint32(len(msg)), prefix[:])
		if errors.Cause(err) != nil {
			t.Fatal(err)
		}
	})
}
