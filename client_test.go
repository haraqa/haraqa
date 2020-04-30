// +build linux

package haraqa

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/haraqa/haraqa/broker"
	"github.com/haraqa/haraqa/internal/mocks"
	"github.com/haraqa/haraqa/internal/protocol"
	"github.com/pkg/errors"
)

type MockConn struct {
	net.Conn
}

var errMock = errors.New("mock error")

func (m *MockConn) Write([]byte) (int, error) {
	return 0, nil
}
func (m *MockConn) Close() error {
	return errMock
}

func TestNewClient(t *testing.T) {
	t.Run("New client w/invalid option", func(t *testing.T) {
		_, err := NewClient(func(*Client) error {
			return errors.New("test")
		})
		if err.Error() != "test" {
			t.Fatal(err)
		}
	})
	t.Run("New client w/invalid address", func(t *testing.T) {
		_, err := NewClient(WithAddr("invalid:address:here"), WithTimeout(time.Second*1))
		if errors.Cause(err) != context.DeadlineExceeded {
			t.Fatal(err)
		}
		c := &Client{}
		_, err = c.dataConnect(nil)
		if err == nil {
			t.Fatal("expected dial error")
		}
	})
}

func newBrokerClient(name string) (*Client, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())

	r := [8]byte{}
	rand.Read(r[:])

	unixSocket := ".haraqa.client.sock" + name + base64.URLEncoding.EncodeToString(r[:])
	volume := ".haraqa.client." + name
	os.RemoveAll(volume)

	b, err := broker.NewBroker(
		broker.WithVolumes([]string{volume}),
		broker.WithLogger(log.New(os.Stdout, "BROKER LOG", log.LstdFlags|log.Lshortfile)),
		broker.WithUnixSocket(unixSocket, os.ModePerm),
		broker.WithGRPCPort(0),
		broker.WithDataPort(0),
	)
	if err != nil {
		return nil, cancel, err
	}
	go func() {
		b.Listen(ctx)
		os.RemoveAll(unixSocket)
	}()
	c, err := NewClient(
		WithGRPCPort(b.GRPCPort),
		WithDataPort(b.DataPort),
		WithTimeout(time.Minute*10),
		WithKeepAlive(time.Millisecond*10),
	)
	return c, cancel, err
}

func TestPing(t *testing.T) {
	c, cancel, err := newBrokerClient(t.Name())
	defer cancel()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * c.keepalive)
	c.Close()
}

func TestCreateTopic(t *testing.T) {
	c, cancel, err := newBrokerClient(t.Name())
	defer cancel()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	err = c.CreateTopic(context.Background(), []byte("test-topic"))
	if err != nil && err != ErrTopicExists {
		t.Fatal(err)
	}

	err = c.CreateTopic(context.Background(), []byte("test-topic"))
	if err != ErrTopicExists {
		t.Fatal(err)
	}

	t.Run("CreateTopic client error", func(t *testing.T) {
		mockClient := mocks.NewMockHaraqaClient(gomock.NewController(t))
		mockClient.EXPECT().CreateTopic(gomock.Any(), &protocol.CreateTopicRequest{
			Topic: []byte("test-topic"),
		}).Return(nil, errMock)
		c := &Client{
			grpcClient: mockClient,
		}
		err := c.CreateTopic(context.Background(), []byte("test-topic"))
		if errors.Cause(err) != errMock {
			t.Fatal(err)
		}
	})
	t.Run("CreateTopic error response", func(t *testing.T) {
		mockClient := mocks.NewMockHaraqaClient(gomock.NewController(t))
		mockClient.EXPECT().CreateTopic(gomock.Any(), &protocol.CreateTopicRequest{Topic: []byte("test-topic")}).
			Return(&protocol.CreateTopicResponse{Meta: &protocol.Meta{ErrorMsg: errMock.Error()}}, nil)
		c := &Client{
			grpcClient: mockClient,
		}
		err := c.CreateTopic(context.Background(), []byte("test-topic"))
		if errors.Cause(err).Error() != errMock.Error() {
			t.Fatal(err)
		}
	})
}

func TestListTopics(t *testing.T) {
	c, cancel, err := newBrokerClient(t.Name())
	defer cancel()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	err = c.CreateTopic(context.Background(), []byte(t.Name()))
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	topics, err := c.ListTopics(ctx, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) == 0 {
		t.Fatal("missing topics")
	}

	// w/regex
	topics, err = c.ListTopics(ctx, "", "", ".*")
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) == 0 {
		t.Fatal("missing topics")
	}

	// w/invalid regex
	_, err = c.ListTopics(ctx, "", "", `\`)
	if err == nil {
		t.Fatal("expected invalid regex err")
	}

	t.Run("List Topics client error", func(t *testing.T) {
		mockClient := mocks.NewMockHaraqaClient(gomock.NewController(t))
		mockClient.EXPECT().ListTopics(gomock.Any(), &protocol.ListTopicsRequest{}).Return(nil, errMock)
		c := &Client{
			grpcClient: mockClient,
		}
		ctx := context.Background()
		_, err := c.ListTopics(ctx, "", "", "")
		if errors.Cause(err) != errMock {
			t.Fatal(err)
		}
	})
	t.Run("List Topics error response", func(t *testing.T) {
		mockClient := mocks.NewMockHaraqaClient(gomock.NewController(t))
		mockClient.EXPECT().ListTopics(gomock.Any(), &protocol.ListTopicsRequest{}).
			Return(&protocol.ListTopicsResponse{Meta: &protocol.Meta{ErrorMsg: errMock.Error()}}, nil)
		c := &Client{
			grpcClient: mockClient,
		}
		ctx := context.Background()
		_, err := c.ListTopics(ctx, "", "", "")
		if errors.Cause(err).Error() != errMock.Error() {
			t.Fatal(err)
		}
	})
}

func TestDeleteTopics(t *testing.T) {
	c, cancel, err := newBrokerClient(t.Name())
	defer cancel()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	topic := []byte(t.Name())
	err = c.CreateTopic(context.Background(), topic)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteTopic(context.Background(), topic)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Delete Topic client error", func(t *testing.T) {
		mockClient := mocks.NewMockHaraqaClient(gomock.NewController(t))
		mockClient.EXPECT().DeleteTopic(gomock.Any(), &protocol.DeleteTopicRequest{Topic: []byte("delete-topic")}).Return(nil, errMock)
		c := &Client{
			grpcClient: mockClient,
		}
		ctx := context.Background()
		err := c.DeleteTopic(ctx, []byte("delete-topic"))
		if errors.Cause(err) != errMock {
			t.Fatal(err)
		}
	})
	t.Run("Delete Topic error response", func(t *testing.T) {
		mockClient := mocks.NewMockHaraqaClient(gomock.NewController(t))
		mockClient.EXPECT().DeleteTopic(gomock.Any(), &protocol.DeleteTopicRequest{Topic: []byte("delete-topic")}).
			Return(&protocol.DeleteTopicResponse{Meta: &protocol.Meta{ErrorMsg: errMock.Error()}}, nil)
		c := &Client{
			grpcClient: mockClient,
		}
		ctx := context.Background()
		err := c.DeleteTopic(ctx, []byte("delete-topic"))
		if errors.Cause(err).Error() != errMock.Error() {
			t.Fatal(err)
		}
	})
}

func TestProduce(t *testing.T) {
	c, cancel, err := newBrokerClient(t.Name())
	defer cancel()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()

	// produce w/ invalid preprocess
	process := []func(msgs [][]byte) error{func(msgs [][]byte) error {
		return errMock
	}}

	p := &Producer{
		c: &Client{
			preProcess: process,
		},
	}
	err = p.Send([]byte("message"))
	if err != errMock {
		t.Fatal(err)
	}

	c.preProcess = process
	err = c.Produce(ctx, nil, []byte("message"))
	if err != errMock {
		t.Fatal(err)
	}
	c.preProcess = nil

	// produce 0 messages
	err = c.Produce(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// invalid topic
	_, err = c.NewProducer(WithTopic(nil), WithContext(ctx))
	if errors.Cause(err).Error() != "missing topic" {
		t.Fatal(err)
	}

	// invalid error handler
	_, err = c.NewProducer(WithErrorHandler(nil))
	if errors.Cause(err).Error() != "invalid error handler" {
		t.Fatal(err)
	}

	// invalid batch size
	_, err = c.NewProducer(WithBatchSize(-1))
	if errors.Cause(err).Error() != "batch size must be greater than 0" {
		t.Fatal(err)
	}

	// send a message without errors
	producer, err := c.NewProducer(
		WithTopic([]byte("producer_send_err")),
		WithIgnoreErrors(),
	)
	if err != nil {
		t.Fatal(err)
	}
	err = producer.Send([]byte("message"))
	if err != nil {
		t.Fatal(err)
	}
	producer.Close()

	// send a message after closing
	err = producer.Send([]byte("message"))
	if err.Error() != "cannot send on closed producer" {
		t.Fatal(err)
	}

	// send a message w/errors
	producer, err = c.NewProducer(
		WithTopic([]byte("producer_send")),
		WithBatchSize(2),
		WithErrorHandler(noopErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	err = producer.Send([]byte("message"))
	if err != nil {
		t.Fatal(err)
	}
}

func TestConsume(t *testing.T) {
	c, cancel, err := newBrokerClient(t.Name())
	defer cancel()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()
	topic := []byte(t.Name())

	// consume w/ topic not exist
	_, err = c.Consume(ctx, topic, 0, 1, nil)
	if errors.Cause(err) != ErrTopicDoesNotExist {
		t.Fatal(err)
	}

	msg := []byte("consume message")
	err = c.Produce(ctx, topic, msg)
	if err != nil {
		t.Fatal(err)
	}

	// consume with err postProcess
	c.postProcess = []func(msgs [][]byte) error{func(msgs [][]byte) error {
		return errMock
	}}
	_, err = c.Consume(ctx, topic, 0, 1, nil)
	if errors.Cause(err) != errMock {
		t.Fatal(err)
	}
	c.postProcess = nil

	// consume message
	msgs, err := c.Consume(ctx, topic, 0, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || !bytes.Equal(msgs[0], msg) {
		t.Fatal(msgs)
	}
}

func TestOffsets(t *testing.T) {
	c, cancel, err := newBrokerClient(t.Name())
	defer cancel()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()
	topic := []byte("offsets_topic")
	err = c.Produce(ctx, topic, []byte("single message"))
	if err != nil {
		t.Fatal(err)
	}

	min, max, err := c.Offsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if min != 0 || max != 1 {
		t.Fatal(min, max)
	}

	topic = []byte("missing_offsets_topic")
	_, _, err = c.Offsets(ctx, topic)
	if err != ErrTopicDoesNotExist {
		t.Fatal(err)
	}

	t.Run("Offsets client error", func(t *testing.T) {
		mockClient := mocks.NewMockHaraqaClient(gomock.NewController(t))
		mockClient.EXPECT().Offsets(gomock.Any(), &protocol.OffsetRequest{
			Topic: []byte("test-topic"),
		}).Return(nil, errMock)
		c := &Client{
			grpcClient: mockClient,
		}
		_, _, err := c.Offsets(context.Background(), []byte("test-topic"))
		if errors.Cause(err) != errMock {
			t.Fatal(err)
		}
	})
	t.Run("Offsets error response", func(t *testing.T) {
		mockClient := mocks.NewMockHaraqaClient(gomock.NewController(t))
		mockClient.EXPECT().Offsets(gomock.Any(), &protocol.OffsetRequest{Topic: []byte("test-topic")}).
			Return(&protocol.OffsetResponse{Meta: &protocol.Meta{ErrorMsg: errMock.Error()}}, nil)
		c := &Client{
			grpcClient: mockClient,
		}
		_, _, err := c.Offsets(context.Background(), []byte("test-topic"))
		if errors.Cause(err).Error() != errMock.Error() {
			t.Fatal(err)
		}
	})
}

func TestWatchTopics(t *testing.T) {
	c, brokerCancel, err := newBrokerClient(t.Name())
	defer brokerCancel()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()
	_, err = c.NewWatcher(ctx)
	if err.Error() != "missing topics from NewWatcher request" {
		t.Fatal(err)
	}

	_, err = c.NewWatcher(ctx, []byte("missingWatchTopic"))
	if err == nil {
		t.Fatal("expected to fail")
	}

	ctx, cancel := context.WithCancel(context.Background())
	topic := []byte("watchTopic")
	err = c.CreateTopic(ctx, topic)
	if err != nil && err != ErrTopicExists {
		t.Fatal(err)
	}

	// start watcher
	w, err := c.NewWatcher(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}

Loop:
	for {
		time.Sleep(time.Millisecond * 10)
		// trigger watch event
		err = c.Produce(ctx, topic, []byte("watch message"))
		if err != nil {
			t.Fatal(err)
		}

		select {
		case event := <-w.Events():
			if !bytes.Equal(event.Topic, topic) {
				t.Fatal()
			}
			break Loop
		default:
		}
	}

	cancel()
	err = w.Close()
	if err != nil && err != context.Canceled {
		t.Fatal(err)
	}

	t.Run("Watch topics client error", func(t *testing.T) {
		mockClient := mocks.NewMockHaraqaClient(gomock.NewController(t))
		mockClient.EXPECT().WatchTopics(gomock.Any()).
			Return(nil, errMock)
		c := &Client{
			grpcClient: mockClient,
		}
		_, err := c.NewWatcher(context.Background(), []byte("test-topic"))
		if errors.Cause(err) != errMock {
			t.Fatal(err)
		}
	})
	t.Run("Watch topics send error", func(t *testing.T) {
		mockStream := mocks.NewMockHaraqa_WatchTopicsClient(gomock.NewController(t))
		mockStream.EXPECT().Send(gomock.Any()).Return(errMock)
		mockClient := mocks.NewMockHaraqaClient(gomock.NewController(t))
		mockClient.EXPECT().WatchTopics(gomock.Any()).
			Return(mockStream, nil)
		c := &Client{
			grpcClient: mockClient,
		}

		_, err := c.NewWatcher(context.Background(), []byte("test-topic"))
		if errors.Cause(err) != errMock {
			t.Fatal(err)
		}
	})
	t.Run("Watch topics recv error", func(t *testing.T) {
		mockStream := mocks.NewMockHaraqa_WatchTopicsClient(gomock.NewController(t))
		mockStream.EXPECT().Send(gomock.Any()).Return(nil)
		mockStream.EXPECT().Recv().Return(nil, errMock)
		mockClient := mocks.NewMockHaraqaClient(gomock.NewController(t))
		mockClient.EXPECT().WatchTopics(gomock.Any()).
			Return(mockStream, nil)
		c := &Client{
			grpcClient: mockClient,
		}

		_, err := c.NewWatcher(context.Background(), []byte("test-topic"))
		if errors.Cause(err) != errMock {
			t.Fatal(err)
		}
	})
	t.Run("Watch topics recv error response", func(t *testing.T) {
		mockStream := mocks.NewMockHaraqa_WatchTopicsClient(gomock.NewController(t))
		mockStream.EXPECT().Send(gomock.Any()).Return(nil)
		mockStream.EXPECT().Recv().Return(&protocol.WatchResponse{Meta: &protocol.Meta{ErrorMsg: errMock.Error()}}, nil)
		mockClient := mocks.NewMockHaraqaClient(gomock.NewController(t))
		mockClient.EXPECT().WatchTopics(gomock.Any()).
			Return(mockStream, nil)
		c := &Client{
			grpcClient: mockClient,
		}

		_, err := c.NewWatcher(context.Background(), []byte("test-topic"))
		if errors.Cause(err).Error() != errMock.Error() {
			t.Fatal(err)
		}
	})
	t.Run("Watch topics recv second error response", func(t *testing.T) {
		mockStream := mocks.NewMockHaraqa_WatchTopicsClient(gomock.NewController(t))
		mockStream.EXPECT().CloseSend().Return(nil)
		mockStream.EXPECT().Send(gomock.Any()).AnyTimes().Return(nil)
		first := true
		mockStream.EXPECT().Recv().AnyTimes().DoAndReturn(func() (*protocol.WatchResponse, error) {
			if first {
				first = false
				return &protocol.WatchResponse{Meta: &protocol.Meta{OK: true}}, nil
			}
			return &protocol.WatchResponse{Meta: &protocol.Meta{ErrorMsg: errMock.Error()}}, nil
		})
		mockClient := mocks.NewMockHaraqaClient(gomock.NewController(t))
		mockClient.EXPECT().WatchTopics(gomock.Any()).
			Return(mockStream, nil)
		c := &Client{
			grpcClient: mockClient,
		}

		w, err := c.NewWatcher(context.Background(), []byte("test-topic"))
		if err != nil {
			t.Fatal(err)
		}
		for event := range w.Events() {
			t.Log(event)
		}
		err = w.Close()
		if errors.Cause(err).Error() != errMock.Error() {
			t.Fatal(err)
		}
	})
	t.Run("Watch topics close", func(t *testing.T) {
		mockStream := mocks.NewMockHaraqa_WatchTopicsClient(gomock.NewController(t))
		mockStream.EXPECT().CloseSend().Return(nil)
		mockStream.EXPECT().Send(gomock.Any()).AnyTimes().Return(nil)
		mockStream.EXPECT().Recv().AnyTimes().Return(&protocol.WatchResponse{Meta: &protocol.Meta{OK: true}}, nil)
		mockClient := mocks.NewMockHaraqaClient(gomock.NewController(t))
		mockClient.EXPECT().WatchTopics(gomock.Any()).
			Return(mockStream, nil)
		c := &Client{
			grpcClient: mockClient,
		}

		w, err := c.NewWatcher(context.Background(), []byte("test-topic"))
		if err != nil {
			t.Fatal(err)
		}
		err = w.Close()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestLock(t *testing.T) {
	c, cancel, err := newBrokerClient(t.Name())
	defer cancel()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()
	groupName := []byte("lock group")
	log.Println("Lock1")
	closer, locked, err := c.Lock(ctx, groupName, true)
	if err != nil {
		t.Fatal(err)
	}
	if !locked {
		t.Fatal("not locked")
	}
	log.Println("Lock1", locked)
	log.Println("Lock2")
	_, locked2, err := c.Lock(ctx, groupName, false)
	if err != nil {
		t.Fatal(err)
	}
	if locked2 {
		t.Fatal("locked")
	}
	log.Println("Lock2", locked2)

	err = closer.Close()
	if err != nil {
		t.Fatal(err)
	}
	log.Println("closed")
	t.Run("Lock client error", func(t *testing.T) {
		mockClient := mocks.NewMockHaraqaClient(gomock.NewController(t))
		mockClient.EXPECT().Lock(gomock.Any()).Return(nil, errMock)
		c := &Client{
			grpcClient: mockClient,
		}
		_, _, err := c.Lock(context.Background(), []byte("group"), true)
		if errors.Cause(err) != errMock {
			t.Fatal(err)
		}
	})
	t.Run("Lock client send error", func(t *testing.T) {
		mockStream := mocks.NewMockHaraqa_LockClient(gomock.NewController(t))
		mockStream.EXPECT().Send(gomock.Any()).Return(errMock)
		mockClient := mocks.NewMockHaraqaClient(gomock.NewController(t))
		mockClient.EXPECT().Lock(gomock.Any()).Return(mockStream, nil)
		c := &Client{
			grpcClient: mockClient,
		}
		_, _, err := c.Lock(context.Background(), []byte("group"), true)
		if errors.Cause(err) != errMock {
			t.Fatal(err)
		}
	})
	t.Run("Lock client send error", func(t *testing.T) {
		mockStream := mocks.NewMockHaraqa_LockClient(gomock.NewController(t))
		mockStream.EXPECT().Send(gomock.Any()).Return(nil)
		mockStream.EXPECT().Recv().Return(nil, errMock)
		mockClient := mocks.NewMockHaraqaClient(gomock.NewController(t))
		mockClient.EXPECT().Lock(gomock.Any()).Return(mockStream, nil)
		c := &Client{
			grpcClient: mockClient,
		}
		_, _, err := c.Lock(context.Background(), []byte("group"), true)
		if errors.Cause(err) != errMock {
			t.Fatal(err)
		}
	})
}

func TestOptions(t *testing.T) {
	t.Run("Address", func(t *testing.T) {
		c := &Client{}
		err := WithAddr("")(c)
		if err.Error() != "invalid host" {
			t.Fatal(err)
		}
		err = WithAddr("127.0.0.1")(c)
		if err != nil {
			t.Fatal(err)
		}
		if c.addr != "127.0.0.1" {
			t.Fail()
		}
	})
	t.Run("GRPC port", func(t *testing.T) {
		c := &Client{}
		err := WithGRPCPort(1234)(c)
		if err != nil {
			t.Fatal(err)
		}
		if c.gRPCPort != 1234 {
			t.Fail()
		}
	})
	t.Run("Data port", func(t *testing.T) {
		c := &Client{}
		err := WithDataPort(1234)(c)
		if err != nil {
			t.Fatal(err)
		}
		if c.dataPort != 1234 {
			t.Fail()
		}
	})
	t.Run("Unix socket", func(t *testing.T) {
		c := &Client{}
		err := WithUnixSocket("/test.tmp")(c)
		if err != nil {
			t.Fatal(err)
		}
		if c.unixSocket != "/test.tmp" {
			t.Fail()
		}
	})
	t.Run("Auto create", func(t *testing.T) {
		c := &Client{}
		err := WithAutoCreateTopics(true)(c)
		if err != nil {
			t.Fatal(err)
		}
		if !c.createTopics {
			t.Fail()
		}
	})
	t.Run("Timeout", func(t *testing.T) {
		c := &Client{}
		err := WithTimeout(time.Second * 3)(c)
		if err != nil {
			t.Fatal(err)
		}
		if c.timeout != time.Second*3 {
			t.Fail()
		}
	})
	t.Run("Keepalive", func(t *testing.T) {
		c := &Client{}
		err := WithKeepAlive(0)(c)
		if err.Error() != "invalid keepalive interval" {
			t.Fatal(err)
		}
		err = WithKeepAlive(time.Second * 5)(c)
		if err != nil {
			t.Fatal(err)
		}
		if c.keepalive != time.Second*5 {
			t.Fail()
		}
	})
	t.Run("Retries", func(t *testing.T) {
		c := &Client{}
		err := WithRetries(-1)(c)
		if err.Error() != "number of retries must be non-negative" {
			t.Fatal(err)
		}
		err = WithRetries(2)(c)
		if err != nil {
			t.Fatal(err)
		}
		if c.retries != 2 {
			t.Fail()
		}
	})
	t.Run("AES", func(t *testing.T) {
		c := &Client{}
		err := WithAESGCM([32]byte{})(c)
		if err != nil {
			t.Fatal(err)
		}
		msgs := [][]byte{[]byte("hello")}
		err = c.preProcess[0](msgs)
		if err != nil {
			t.Fatal(err)
		}
		err = c.postProcess[0](msgs)
		if err != nil {
			t.Fatal(err)
		}

		err = c.postProcess[0](msgs)
		if err.Error() != "invalid message size" {
			t.Fatal(err)
		}
		msgs[0] = make([]byte, 20)
		err = c.postProcess[0](msgs)
		if err.Error() != "cipher: message authentication failed" {
			t.Fatal(err)
		}
	})
}
