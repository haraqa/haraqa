package haraqa

import (
	"bytes"
	"context"
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

func (m *MockConn) Close() error {
	return errMock
}

func TestAll(t *testing.T) {
	b, err := broker.NewBroker(broker.DefaultConfig)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		err := b.Listen(ctx)
		if err != nil && err != ctx.Err() {
			t.Log(err)
		}
	}()

	defer func() {
		_ = os.RemoveAll(broker.DefaultConfig.Volumes[0])
	}()

	t.Run("New client", testNewClient)
	c, err := NewClient(WithTimeout(time.Second * 1))
	if err != nil {
		t.Fatal(err)
	}
	t.Run("Create Topic", testCreateTopic(c))
	t.Run("List Topics", testListTopics(c))
	t.Run("Delete Topics", testDeleteTopics(c))
	t.Run("Produce", testProduce(c))
	t.Run("Consume", testConsume(c))
	t.Run("Offsets", testOffsets(c))
	t.Run("WatchTopic", testWatchTopics(c))
	t.Run("Lock", testLock(c))
	t.Run("Options", testOptions)
}

func testNewClient(t *testing.T) {
	t.Run("New client w/valid option", func(t *testing.T) {
		c, err := NewClient(WithTimeout(time.Second * 2))
		if err != nil {
			t.Fatal(err)
		}
		err = c.Close()
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("New client w/no options", func(t *testing.T) {
		c, err := NewClient()
		if err != nil {
			t.Fatal(err)
		}
		err = c.Close()
		if err != nil {
			t.Fatal(err)
		}
	})
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
		err = c.dataConnect()
		if err == nil {
			t.Fatal("expected dial error")
		}
	})
	t.Run("New client w/unix socket", func(t *testing.T) {
		c, err := NewClient(WithUnixSocket("tmp/sock.sock"), WithTimeout(time.Second*1))
		if err != nil {
			t.Fatal(err)
		}
		err = c.dataConnect()
		if err == nil {
			t.Fatal("expected missing file error")
		}
	})
	t.Run("Close with error", func(t *testing.T) {
		c := Client{
			dataConn: &MockConn{},
		}
		err := c.Close()
		if errors.Cause(err) != errMock {
			t.Fatal(err)
		}
	})
}

func testCreateTopic(c *Client) func(t *testing.T) {
	return func(t *testing.T) {
		err := c.CreateTopic(context.Background(), []byte("test-topic"))
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
}

func testListTopics(c *Client) func(t *testing.T) {
	return func(t *testing.T) {
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
}

func testDeleteTopics(c *Client) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		topics, err := c.ListTopics(ctx, "", "", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(topics) == 0 {
			t.Fatal("missing topics")
		}
		for _, topic := range topics {
			err = c.DeleteTopic(ctx, topic)
			if err != nil {
				t.Fatal(err)
			}
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
}

func testProduce(c *Client) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		// invalid channel
		var ch chan ProduceMsg
		err := c.ProduceLoop(ctx, []byte("test-topic"), ch)
		if err.Error() != "invalid channel capacity, channels must have a capacity of at least 1" {
			t.Fatal(err)
		}

		// invalid channel length
		ch = make(chan ProduceMsg)
		err = c.ProduceLoop(ctx, []byte("test-topic"), ch)
		if err.Error() != "invalid channel capacity, channels must have a capacity of at least 1" {
			t.Fatal(err)
		}

		// produce 0 messages
		err = c.Produce(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}

		// produce
		ctx, cancel := context.WithCancel(ctx)
		ch = make(chan ProduceMsg, 1)
		go func() {
			err := c.ProduceLoop(ctx, []byte("test-topic"), ch)
			if err != nil {
				t.Log(err)
			}
		}()
		msg := NewProduceMsg([]byte("hello world"))
		ch <- msg
		err = <-msg.Err
		if err != nil {
			t.Fatal(err)
		}
		cancel()

		// produce loop w/invalid preProcess
		c.preProcess = []func(msgs [][]byte) error{func(msgs [][]byte) error {
			return errMock
		}}
		ctx, cancel = context.WithCancel(context.Background())
		go func() {
			err := c.ProduceLoop(ctx, []byte("test-topic"), ch)
			if err != nil {
				t.Log(err)
			}
		}()
		msg = NewProduceMsg([]byte("hello world"))
		ch <- msg
		err = <-msg.Err
		if err != errMock {
			t.Fatal(err)
		}
		cancel()

		// produce w/ invalid preprocess
		err = c.Produce(ctx, nil, []byte("message"))
		if err != errMock {
			t.Fatal(err)
		}

		c.preProcess = nil

		// produce w/ invalid dataconn
		c.dataConn = nil
		dataport := c.dataPort
		c.dataPort = -1
		err = c.Produce(ctx, nil, []byte("message"))
		if _, ok := errors.Cause(err).(*net.OpError); !ok {
			t.Fatal(err)
		}

		// produce loop w/invalid dataconn
		err = c.ProduceLoop(ctx, []byte("test-topic"), ch)
		if _, ok := errors.Cause(err).(*net.OpError); !ok {
			t.Fatal(err)
		}

		c.dataPort = dataport
	}
}

func testConsume(c *Client) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		topic := []byte("consumeTopic")
		msg := []byte("consume message")
		err := c.Produce(ctx, topic, msg)
		if err != nil {
			t.Fatal(err)
		}

		// consume w/invalid dataconn
		dataPort := c.dataPort
		c.dataConn = nil
		c.dataPort = -1
		_, err = c.Consume(ctx, topic, 0, 1, nil)
		if _, ok := errors.Cause(err).(*net.OpError); !ok {
			t.Fatal(err)
		}
		c.dataPort = dataPort

		// consume
		msgs, err := c.Consume(ctx, topic, 0, 1, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) != 1 || !bytes.Equal(msgs[0], msg) {
			t.Fatal(msgs)
		}

		// consume with closed datacon
		c.dataConn.Close()
		_, err = c.Consume(ctx, topic, 0, 1, nil)
		if err != nil {
			t.Fatal(err)
		}

		// consume with err postProcess
		c.postProcess = []func(msgs [][]byte) error{func(msgs [][]byte) error {
			return errMock
		}}
		_, err = c.Consume(ctx, topic, 0, 1, nil)
		if err != errMock {
			t.Fatal(err)
		}
		c.postProcess = nil
	}
}

func testOffsets(c *Client) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		topic := []byte("offsets_topic")
		err := c.Produce(ctx, topic, []byte("single message"))
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
}

func testWatchTopics(c *Client) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		ch := make(chan WatchEvent, 1)
		err := c.WatchTopics(ctx, ch)
		if err.Error() != "invalid number of topics sent" {
			t.Fatal(err)
		}

		err = c.WatchTopics(ctx, ch, []byte("missingWatchTopic"))
		if err == nil {
			t.Fatal("expected to pass")
		}

		ctx, cancel := context.WithCancel(context.Background())
		topic := []byte("watchTopic")
		err = c.CreateTopic(ctx, topic)
		if err != nil && err != ErrTopicExists {
			t.Fatal(err)
		}

		// start watcher
		errs := make(chan error, 1)
		go func() {
			errs <- c.WatchTopics(ctx, ch, topic)
		}()

	Loop:
		for {
			time.Sleep(time.Millisecond * 10)
			// trigger watch event
			err = c.Produce(ctx, topic, []byte("watch message"))
			if err != nil {
				t.Fatal(err)
			}

			select {
			case event := <-ch:
				if !bytes.Equal(event.Topic, topic) {
					t.Fatal()
				}
				break Loop
			default:
			}
		}

		cancel()
		err = <-errs
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
			ch := make(chan WatchEvent, 1)
			err := c.WatchTopics(context.Background(), ch, []byte("test-topic"))
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

			ch := make(chan WatchEvent, 1)
			err := c.WatchTopics(context.Background(), ch, []byte("test-topic"))
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

			ch := make(chan WatchEvent, 1)
			err := c.WatchTopics(context.Background(), ch, []byte("test-topic"))
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

			ch := make(chan WatchEvent, 1)
			err := c.WatchTopics(context.Background(), ch, []byte("test-topic"))
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

			ch := make(chan WatchEvent, 1)
			err := c.WatchTopics(context.Background(), ch, []byte("test-topic"))
			if errors.Cause(err).Error() != errMock.Error() {
				t.Fatal(err)
			}
		})
	}
}

func testLock(c *Client) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		groupName := []byte("lock group")
		closer, locked, err := c.Lock(ctx, groupName, true)
		if err != nil {
			t.Fatal(err)
		}
		if !locked {
			t.Fatal("not locked")
		}

		_, locked2, err := c.Lock(ctx, groupName, false)
		if err != nil {
			t.Fatal(err)
		}
		if locked2 {
			t.Fatal("locked")
		}

		err = closer.Close()
		if err != nil {
			t.Fatal(err)
		}
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
}

func testOptions(t *testing.T) {
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
