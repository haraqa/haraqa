package broker

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/haraqa/haraqa/internal/mocks"
	"github.com/haraqa/haraqa/internal/protocol"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"gopkg.in/fsnotify.v1"
)

func TestGRPCServer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		errMock = errors.New("mock error")
		ctx     = context.Background()
		b       = &Broker{}
	)

	t.Run("CreateTopic", func(t *testing.T) {
		mockQ := mocks.NewMockQueue(ctrl)
		gomock.InOrder(
			mockQ.EXPECT().CreateTopic(gomock.Any()).Return(nil),
			mockQ.EXPECT().CreateTopic(gomock.Any()).Return(errMock),
		)
		b.Q = mockQ

		in := &protocol.CreateTopicRequest{
			Topic: []byte("create-topic"),
		}
		resp, err := b.CreateTopic(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		if !resp.GetMeta().GetOK() {
			t.Fatal(resp.GetMeta())
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
		mockQ := mocks.NewMockQueue(ctrl)
		b.Q = mockQ

		gomock.InOrder(
			mockQ.EXPECT().DeleteTopic(gomock.Any()).Return(nil),
			mockQ.EXPECT().DeleteTopic(gomock.Any()).Return(errMock),
		)

		in := &protocol.DeleteTopicRequest{
			Topic: []byte("delete-topic"),
		}
		resp, err := b.DeleteTopic(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		if !resp.GetMeta().GetOK() {
			t.Fatal(resp.GetMeta())
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

	t.Run("TruncateTopic", func(t *testing.T) {
		mockQ := mocks.NewMockQueue(ctrl)
		b.Q = mockQ
		offset := int64(13)
		topic := []byte("truncate-topic")
		before := time.Now().Round(time.Second)

		gomock.InOrder(
			mockQ.EXPECT().TruncateTopic(topic, offset, before).Return(nil),
			mockQ.EXPECT().TruncateTopic(topic, offset, before).Return(errMock),
		)

		in := &protocol.TruncateTopicRequest{
			Topic:  topic,
			Offset: offset,
			Time:   before.Unix(),
		}
		resp, err := b.TruncateTopic(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		if !resp.GetMeta().GetOK() {
			t.Fatal(resp.GetMeta())
		}

		resp, err = b.TruncateTopic(ctx, in)
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
		mockQ := mocks.NewMockQueue(ctrl)
		b.Q = mockQ
		gomock.InOrder(
			mockQ.EXPECT().ListTopics("", "", "").Return(nil, nil),
			mockQ.EXPECT().ListTopics("", "", "").Return(nil, errMock),
		)

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
		mockQ := mocks.NewMockQueue(ctrl)
		b.Q = mockQ
		gomock.InOrder(
			mockQ.EXPECT().Offsets(gomock.Any()).Return(int64(0), int64(1), nil),
			mockQ.EXPECT().Offsets(gomock.Any()).Return(int64(0), int64(0), errMock),
			mockQ.EXPECT().Offsets(gomock.Any()).Return(int64(0), int64(0), os.ErrNotExist),
		)

		in := &protocol.OffsetRequest{
			Topic: []byte("offsets-topic"),
		}
		resp, err := b.Offsets(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		if !resp.GetMeta().GetOK() {
			t.Fatal(resp.GetMeta())
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
		b.groupLocks = make(map[string]chan struct{})
		lockTrue := &protocol.LockRequest{
			Group: []byte("lock-group"),
			Lock:  true,
			Time:  5000,
		}
		lockFalse := &protocol.LockRequest{
			Group: []byte("lock-group"),
			Lock:  false,
			Time:  5000,
		}

		mockLock := mocks.NewMockHaraqa_LockServer(ctrl)
		gomock.InOrder(
			mockLock.EXPECT().Recv().Return(nil, errMock),

			mockLock.EXPECT().Recv().Return(lockTrue, nil),
			mockLock.EXPECT().Send(gomock.Any()).Return(errMock),

			mockLock.EXPECT().Recv().Return(lockTrue, nil),
			mockLock.EXPECT().Send(gomock.Any()).Return(nil),
			mockLock.EXPECT().Recv().Return(lockFalse, nil),
			mockLock.EXPECT().Send(gomock.Any()).Return(nil),
			mockLock.EXPECT().Recv().Return(nil, grpc.ErrServerStopped),
		)

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
	t.Run("WatchTopic", func(t *testing.T) {
		t.Run("Pre-queue", func(t *testing.T) {
			mockWatch := mocks.NewMockHaraqa_WatchTopicsServer(ctrl)
			gomock.InOrder(
				mockWatch.EXPECT().Recv().Return(nil, errMock),
				mockWatch.EXPECT().Send(gomock.Any()).Return(nil),

				mockWatch.EXPECT().Recv().Return(&protocol.WatchRequest{}, nil),

				mockWatch.EXPECT().Recv().Return(&protocol.WatchRequest{Topics: [][]byte{[]byte("*")}}, nil),
				mockWatch.EXPECT().Send(gomock.Any()).Return(nil),
			)
			err := b.WatchTopics(mockWatch)
			if err != errMock {
				t.Fatal(err)
			}
			err = b.WatchTopics(mockWatch)
			if err != nil {
				t.Fatal(err)
			}
			b.Volumes = []string{""}
			err = b.WatchTopics(mockWatch)
			if errors.Cause(err).Error() != "no such file or directory" {
				t.Fatal(err)
			}
		})

		watchFileName := ".haraqa.watch"
		_, err := os.Create(watchFileName)
		if err != nil {
			t.Fatal(err)
		}
		t.Run("Queue", func(t *testing.T) {
			mockQ := mocks.NewMockQueue(ctrl)
			b.Q = mockQ
			mockWatch := mocks.NewMockHaraqa_WatchTopicsServer(ctrl)
			gomock.InOrder(
				mockWatch.EXPECT().Recv().Return(&protocol.WatchRequest{Topics: [][]byte{[]byte(watchFileName)}}, nil),
				mockQ.EXPECT().Offsets(gomock.Any()).Return(int64(0), int64(0), errMock),
				mockWatch.EXPECT().Send(gomock.Any()).Return(nil),

				mockWatch.EXPECT().Recv().Return(&protocol.WatchRequest{Topics: [][]byte{[]byte(watchFileName)}}, nil),
				mockQ.EXPECT().Offsets(gomock.Any()).Return(int64(0), int64(0), os.ErrNotExist),
				mockWatch.EXPECT().Send(gomock.Any()).Return(errMock),

				mockWatch.EXPECT().Recv().Return(&protocol.WatchRequest{Topics: [][]byte{[]byte(watchFileName)}}, nil),
				mockQ.EXPECT().Offsets(gomock.Any()).Return(int64(0), int64(1), nil),
				mockWatch.EXPECT().Send(gomock.Any()).Return(nil),
				mockWatch.EXPECT().Send(gomock.Any()).Return(errMock),

				mockWatch.EXPECT().Recv().Return(&protocol.WatchRequest{Topics: [][]byte{[]byte(watchFileName)}}, nil),
				mockQ.EXPECT().Offsets(gomock.Any()).Return(int64(0), int64(-1), nil),
				mockWatch.EXPECT().Send(gomock.Any()).Return(nil),
				mockWatch.EXPECT().Recv().Return(nil, errMock),
				mockWatch.EXPECT().Send(gomock.Any()).Return(nil),

				mockWatch.EXPECT().Recv().Return(&protocol.WatchRequest{Topics: [][]byte{[]byte(watchFileName)}}, nil),
				mockQ.EXPECT().Offsets(gomock.Any()).Return(int64(0), int64(-1), nil),
				mockWatch.EXPECT().Send(gomock.Any()).Return(nil),
				mockWatch.EXPECT().Recv().Return(&protocol.WatchRequest{Term: true}, nil),
			)
			err := b.WatchTopics(mockWatch)
			if errors.Cause(err) != errMock {
				t.Fatal(err)
			}
			err = b.WatchTopics(mockWatch)
			if errors.Cause(err) != errMock {
				t.Fatal(err)
			}
			err = b.WatchTopics(mockWatch)
			if errors.Cause(err) != errMock {
				t.Fatal(err)
			}
			err = b.WatchTopics(mockWatch)
			if errors.Cause(err) != errMock {
				t.Fatal(err)
			}
			err = b.WatchTopics(mockWatch)
			if errors.Cause(err) != nil {
				t.Fatal(err)
			}
		})
		t.Run("watch", func(t *testing.T) {
			mockWatch := mocks.NewMockHaraqa_WatchTopicsServer(ctrl)
			errs := make(chan error)
			offsets := map[string][2]int64{
				".haraqa.valid": {0, 0},
			}
			watchEvents := make(chan fsnotify.Event)
			go b.watch(mockWatch, watchEvents, nil, errs, offsets)
			watchEvents <- fsnotify.Event{
				Op:   fsnotify.Create,
				Name: "invalid/invalid.dat",
			}
			watchEvents <- fsnotify.Event{
				Op:   fsnotify.Write,
				Name: "invalid/invalid.dat",
			}
			watchEvents <- fsnotify.Event{
				Op:   fsnotify.Write,
				Name: ".haraqa.valid/invalid.dat",
			}
			err := <-errs
			if !strings.HasSuffix(errors.Cause(err).Error(), "no such file or directory") {
				t.Fatal(err)
			}

			os.RemoveAll(".haraqa.valid")
			err = os.Mkdir(".haraqa.valid", 0777)
			if err != nil {
				t.Fatal(err)
			}
			_, err = os.Create(".haraqa.valid/valid.dat")
			if err != nil {
				t.Fatal(err)
			}
			mockWatch.EXPECT().Send(gomock.Any()).Return(errMock)
			go b.watch(mockWatch, watchEvents, nil, errs, offsets)
			watchEvents <- fsnotify.Event{
				Op:   fsnotify.Write,
				Name: ".haraqa.valid/valid.dat",
			}
			err = <-errs
			if errors.Cause(err) != errMock {
				t.Fatal(err)
			}
		})
		t.Run("Produce", func(t *testing.T) {
			mockQ := mocks.NewMockQueue(ctrl)
			b.Q = mockQ
			topic := []byte("grpc-produce")
			msgSizes := []int64{5, 6}
			gomock.InOrder(
				mockQ.EXPECT().Produce(gomock.Any(), topic, msgSizes).DoAndReturn(func(r io.Reader, b []byte, ms []int64) error {
					var totalSize int64
					for i := range ms {
						totalSize += ms[i]
					}
					buf := make([]byte, totalSize)
					_, err := io.ReadFull(r, buf)
					return err
				}),
				mockQ.EXPECT().Produce(gomock.Any(), topic, msgSizes).Return(errMock),
			)

			in := &protocol.GRPCProduceRequest{
				Topic:    topic,
				MsgSizes: msgSizes,
				Messages: []byte("hello world"),
			}
			resp, err := b.Produce(ctx, in)
			if err != nil {
				t.Fatal(err)
			}
			if !resp.GetMeta().GetOK() {
				t.Fatal(resp.GetMeta())
			}

			resp, err = b.Produce(ctx, in)
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
		t.Run("Consume", func(t *testing.T) {
			mockQ := mocks.NewMockQueue(ctrl)
			b.Q = mockQ
			var (
				topic     = []byte("grpc-consume")
				msgSizes  = []int64{5, 6}
				totalSize = int64(11)
				filename  = []byte("filename.hrq")
				startAt   = int64(17)
				offset    = int64(7)
				limit     = int64(13)
			)

			gomock.InOrder(
				mockQ.EXPECT().ConsumeInfo(topic, offset, limit).Return(filename, startAt, msgSizes, nil),
				mockQ.EXPECT().Consume(gomock.Any(), topic, filename, startAt, totalSize).Return(nil),

				mockQ.EXPECT().ConsumeInfo(topic, offset, limit).Return(filename, startAt, msgSizes, errMock),

				mockQ.EXPECT().ConsumeInfo(topic, offset, limit).Return(filename, startAt, msgSizes, nil),
				mockQ.EXPECT().Consume(gomock.Any(), topic, filename, startAt, totalSize).Return(errMock),
			)

			in := &protocol.GRPCConsumeRequest{
				Topic:  topic,
				Offset: offset,
				Limit:  limit,
			}
			resp, err := b.Consume(ctx, in)
			if err != nil {
				t.Fatal(err)
			}
			if !resp.GetMeta().GetOK() {
				t.Fatal(resp.GetMeta())
			}

			resp, err = b.Consume(ctx, in)
			if err != nil {
				t.Fatal(err)
			}
			if resp.GetMeta().GetOK() {
				t.Fatal(resp.GetMeta())
			}
			if resp.GetMeta().GetErrorMsg() != errMock.Error() {
				t.Fatal(resp.GetMeta().GetErrorMsg())
			}

			resp, err = b.Consume(ctx, in)
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
	})
}
