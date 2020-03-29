package broker

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/haraqa/haraqa/internal/mocks"
	"github.com/haraqa/haraqa/internal/protocol"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var errMock = errors.New("mock error")

func TestGRPCServer(t *testing.T) {
	ctx := context.Background()
	mockQ := mocks.NewMockQueue(gomock.NewController(t))
	b := &Broker{
		Q:          mockQ,
		groupLocks: make(map[string]chan struct{}),
		config: Config{
			Volumes: []string{".haraqa-watch"},
		},
	}

	t.Run("CreateTopic", func(t *testing.T) {
		topic := []byte("create-topic")
		mockQ.EXPECT().CreateTopic(topic).Return(nil)
		in := &protocol.CreateTopicRequest{
			Topic: topic,
		}
		resp, err := b.CreateTopic(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		if !resp.GetMeta().GetOK() {
			t.Fatal(resp.GetMeta())
		}

		topic2 := []byte("create-topic-2")
		mockQ.EXPECT().CreateTopic(topic2).Return(errMock)
		in = &protocol.CreateTopicRequest{
			Topic: topic2,
		}
		resp, err = b.CreateTopic(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		if resp.GetMeta().GetOK() {
			t.Fatal(resp.GetMeta())
		}
		if resp.GetMeta().GetErrorMsg() != errMock.Error() {
			t.Fatal(resp.GetMeta().GetErrorMsg())
		}
	})
	t.Run("DeleteTopic", func(t *testing.T) {
		topic := []byte("delete-topic")
		mockQ.EXPECT().DeleteTopic(topic).Return(nil)
		in := &protocol.DeleteTopicRequest{
			Topic: topic,
		}
		resp, err := b.DeleteTopic(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		if !resp.GetMeta().GetOK() {
			t.Fatal(resp.GetMeta())
		}

		topic2 := []byte("delete-topic-2")
		mockQ.EXPECT().DeleteTopic(topic2).Return(errMock)
		in = &protocol.DeleteTopicRequest{
			Topic: topic2,
		}
		resp, err = b.DeleteTopic(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		if resp.GetMeta().GetOK() {
			t.Fatal(resp.GetMeta())
		}
		if resp.GetMeta().GetErrorMsg() != errMock.Error() {
			t.Fatal(resp.GetMeta().GetErrorMsg())
		}
	})
	t.Run("ListTopics", func(t *testing.T) {
		first := true
		mockQ.EXPECT().ListTopics("", "", "").Times(2).
			DoAndReturn(func(prefix, suffix, regex string) (*protocol.ListTopicsResponse, error) {
				if first {
					first = false
					return nil, nil
				}
				return nil, errMock
			})
		in := &protocol.ListTopicsRequest{}
		resp, err := b.ListTopics(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		if !resp.GetMeta().GetOK() {
			t.Fatal(resp.GetMeta())
		}

		resp, err = b.ListTopics(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		if resp.GetMeta().GetOK() {
			t.Fatal(resp.GetMeta())
		}
		if resp.GetMeta().GetErrorMsg() != errMock.Error() {
			t.Fatal(resp.GetMeta().GetErrorMsg())
		}
	})
	t.Run("Offsets", func(t *testing.T) {
		topic := []byte("offsets-topic")
		mockQ.EXPECT().Offsets(topic).Return(int64(0), int64(1), nil)
		in := &protocol.OffsetRequest{
			Topic: topic,
		}
		resp, err := b.Offsets(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		if !resp.GetMeta().GetOK() {
			t.Fatal(resp.GetMeta())
		}

		topic2 := []byte("offsets-topic-2")
		mockQ.EXPECT().Offsets(topic2).Return(int64(0), int64(0), errMock)
		in = &protocol.OffsetRequest{
			Topic: topic2,
		}
		resp, err = b.Offsets(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		if resp.GetMeta().GetOK() {
			t.Fatal(resp.GetMeta())
		}
		if resp.GetMeta().GetErrorMsg() != errMock.Error() {
			t.Fatal(resp.GetMeta().GetErrorMsg())
		}

		topic3 := []byte("offsets-topic-3")
		mockQ.EXPECT().Offsets(topic3).Return(int64(0), int64(0), os.ErrNotExist)
		in = &protocol.OffsetRequest{
			Topic: topic3,
		}
		resp, err = b.Offsets(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		if resp.GetMeta().GetOK() {
			t.Fatal(resp.GetMeta())
		}
		if resp.GetMeta().GetErrorMsg() != protocol.ErrTopicDoesNotExist.Error() {
			t.Fatal(resp.GetMeta().GetErrorMsg())
		}
	})
	t.Run("Lock", func(t *testing.T) {
		mockLock := mocks.NewMockHaraqa_LockServer(gomock.NewController(t))
		recvCount := 0
		mockLock.EXPECT().Recv().AnyTimes().
			DoAndReturn(func() (*protocol.LockRequest, error) {
				defer func() { recvCount++; log.Println("recv", recvCount) }()
				switch recvCount {
				case 0:
					return nil, errMock
				case 1, 2, 3:
					return &protocol.LockRequest{
						Group: []byte("lock-group"),
						Lock:  true,
						Time:  5000,
					}, nil
				case 4:
					return &protocol.LockRequest{
						Group: []byte("lock-group"),
						Lock:  false,
						Time:  5000,
					}, nil
				case 5:
					return nil, grpc.ErrServerStopped
				}
				panic("unexpected recv")
			})
		sendCount := 0
		mockLock.EXPECT().Send(gomock.Any()).AnyTimes().
			DoAndReturn(func(*protocol.LockResponse) error {
				defer func() { sendCount++; log.Println("send", sendCount) }()
				switch sendCount {
				case 0:
					return errMock
				case 1, 2, 3:
					return nil
				}
				panic("unexpected send")
			})

		err := b.Lock(mockLock)
		if err != errMock {
			t.Fatal(err)
		}
		err = b.Lock(mockLock)
		if err != errMock {
			t.Fatal(err)
		}
		err = b.Lock(mockLock)
		if err != grpc.ErrServerStopped {
			t.Fatal(err)
		}
	})
	t.Run("Watch", func(t *testing.T) {
		topic := []byte("watch-topic")
		block := make(chan struct{})
		mockWatch := mocks.NewMockHaraqa_WatchTopicsServer(gomock.NewController(t))
		recvCount := 0
		mockWatch.EXPECT().Recv().AnyTimes().
			DoAndReturn(func() (*protocol.WatchRequest, error) {
				defer func() { recvCount++; log.Println("recv", recvCount) }()
				switch recvCount {
				case 0:
					return nil, errMock
				case 1:
					return &protocol.WatchRequest{}, nil
				case 2, 3, 4:
					return &protocol.WatchRequest{
						Topics: [][]byte{topic},
					}, nil
				case 5:
					// write to file
					dat, err := os.Create(filepath.Join(b.config.Volumes[0], string(topic), "0000000000000000.dat"))
					if err != nil {
						t.Fatal(err)
					}
					data := [24]byte{}
					_, err = dat.Write(data[:])
					if err != nil {
						t.Fatal(err)
					}
					<-block
					// end connection
					return &protocol.WatchRequest{
						Topics: [][]byte{topic},
						Term:   true,
					}, nil
				}
				panic("unexpected recv")
			})
		sendCount := 0
		mockWatch.EXPECT().Send(gomock.Any()).AnyTimes().
			DoAndReturn(func(resp *protocol.WatchResponse) error {
				defer func() { sendCount++; log.Println("send", sendCount) }()
				switch sendCount {
				case 0:
					return nil
				case 1, 2:
					if resp.GetMeta().GetOK() {
						t.Fatal("expected error")
					}
					return nil
				case 3, 4:
					return nil
				case 5:
					defer close(block)
					return errMock
				case 6:
					return nil
				}
				panic("unexpected send")
			})
		err := b.WatchTopics(mockWatch)
		if err != errMock {
			t.Fatal(err)
		}
		err = b.WatchTopics(mockWatch)
		if err != nil {
			t.Fatal(err)
		}
		err = b.WatchTopics(mockWatch)
		if err == nil {
			t.Fatal("expected error")
		}
		err = os.MkdirAll(filepath.Join(b.config.Volumes[0], string(topic)), 0777)
		if err != nil {
			t.Fatal(err)
		}
		offsetCount := 0
		mockQ.EXPECT().Offsets(topic).AnyTimes().DoAndReturn(func([]byte) (int64, int64, error) {
			defer func() { offsetCount++; log.Println("send", sendCount) }()
			switch offsetCount {
			case 0:
				return 0, 0, errMock
			case 1:
				return 0, 1, nil
			}
			panic("unexpected offsets")
		})
		err = b.WatchTopics(mockWatch)
		if errors.Cause(err) != errMock {
			t.Fatal(err)
		}
		err = b.WatchTopics(mockWatch)
		if errors.Cause(err) != errMock {
			t.Fatal(err)
		}
	})
}
