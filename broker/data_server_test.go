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
	var errMock = errors.New("mock error")
	b := &Broker{}

	conn := mocks.NewMockReadWriteCloser(gomock.NewController(t))
	conn.EXPECT().Close().AnyTimes().Return(nil)
	readCount := 0
	conn.EXPECT().Read(gomock.Any()).AnyTimes().
		DoAndReturn(func(b []byte) (int, error) {
			defer func() { readCount++ }()
			switch readCount {
			case 0:
				return 0, io.EOF
			case 1:
				return 0, errMock
			case 2:
				copy(b, []byte{0, protocol.TypeClose, 0, 0, 0, 0})
				return 6, nil
			case 3, 4:
				copy(b, []byte{0, protocol.TypePing, 0, 0, 0, 0})
				return 6, nil
			case 5:
				copy(b, []byte{0, protocol.TypeProduce, 0, 0, 0, 0})
				return 6, nil
			case 6:
				copy(b, []byte{0, protocol.TypeConsume, 0, 0, 0, 0})
				return 6, nil
			}
			panic("unexpected read")
		})
	writeCount := 0
	conn.EXPECT().Write(gomock.Any()).AnyTimes().
		DoAndReturn(func(b []byte) (int, error) {
			defer func() { writeCount++ }()
			switch writeCount {
			case 0:
				return 0, nil
			case 1:
				return 0, errMock
			case 2:
				return 6, nil
			case 3, 4, 5:
				return 0, nil
			}
			panic("unexpected write")
		})

		// read eof
	b.handleDataConn(conn)
	// read err
	b.handleDataConn(conn)
	// close prefix
	b.handleDataConn(conn)
	// ping prefix (error)
	b.handleDataConn(conn)
	// ping prefix (success) & produce prefix (error)
	b.handleDataConn(conn)
	// consume prefix (error)
	b.handleDataConn(conn)
}

func TestProduceHandler(t *testing.T) {
	var errMock = errors.New("mock error")

	req1 := &protocol.ProduceRequest{
		Topic:    []byte("produce-topic"),
		MsgSizes: []int64{5, 6},
	}
	w1 := new(bytes.Buffer)
	err := req1.Write(w1)
	if err != nil {
		t.Fatal(err)
	}
	msg1 := w1.Bytes()[6:]

	req2 := &protocol.ProduceRequest{
		Topic:    []byte("produce-topic-2"),
		MsgSizes: []int64{5, 6},
	}
	w2 := new(bytes.Buffer)
	err = req2.Write(w2)
	if err != nil {
		t.Fatal(err)
	}
	msg2 := w2.Bytes()[6:]

	mockQ := mocks.NewMockQueue(gomock.NewController(t))
	mockQ.EXPECT().Produce(gomock.Any(), req1.Topic, gomock.Any()).Return(errMock)
	mockQ.EXPECT().Produce(gomock.Any(), req2.Topic, gomock.Any()).Times(2).Return(nil)
	b := &Broker{
		Q:      mockQ,
		config: Config{MaxSize: 200},
	}
	produceReq := &protocol.ProduceRequest{}
	var buf []byte
	var prefix [6]byte

	conn := mocks.NewMockReadWriteCloser(gomock.NewController(t))
	readCount := 0
	conn.EXPECT().Read(gomock.Any()).AnyTimes().
		DoAndReturn(func(b []byte) (int, error) {
			defer func() { readCount++ }()
			switch readCount {
			case 0:
				return 0, errMock
			case 1:
				copy(b, []byte{0, 0, 0, 0, 0})
				return 5, nil
			case 2:
				n := copy(b, msg1)
				return n, nil
			case 3, 4:
				n := copy(b, msg2)
				return n, nil
				//	case 3:
				//		copy(b, w2.Bytes())
				//		return w2.Len(), nil
			}
			panic("unexpected read")
		})
	writeCount := 0
	conn.EXPECT().Write(gomock.Any()).AnyTimes().
		DoAndReturn(func(b []byte) (int, error) {
			defer func() { writeCount++ }()
			switch writeCount {
			case 0, 1, 2:
				return 0, nil
			case 3:
				return 0, errMock
			case 4, 5:
				return 6, nil
			}
			panic("unexpected write")
		})

	err = b.handleProduce(conn, produceReq, &buf, 5, prefix[:])
	if errors.Cause(err) != errMock {
		t.Fatal(err)
	}
	err = b.handleProduce(conn, produceReq, &buf, 5, prefix[:])
	if errors.Cause(err).Error() != "invalid messages length" {
		t.Fatal(err)
	}
	err = b.handleProduce(conn, produceReq, &buf, uint32(len(msg1)), prefix[:])
	if errors.Cause(err) != errMock {
		t.Fatal(err)
	}
	err = b.handleProduce(conn, produceReq, &buf, uint32(len(msg2)), prefix[:])
	if errors.Cause(err) != errMock {
		t.Fatal(err)
	}
	err = b.handleProduce(conn, produceReq, &buf, uint32(len(msg2)), prefix[:])
	if errors.Cause(err) != nil {
		t.Fatal(err)
	}
}
