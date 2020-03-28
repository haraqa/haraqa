package haraqa

import (
	"bytes"
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/haraqa/haraqa/broker"
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
	go func() {
		err := b.Listen()
		if err != nil {
			t.Log(err)
		}
		b.Close()
	}()

	defer func() {
		os.RemoveAll(broker.DefaultConfig.Volumes[0])
	}()

	t.Run("New client", testNewClient)
	t.Run("Create Topic", testCreateTopic)
	t.Run("List Topics", testListTopics)
	t.Run("Delete Topics", testDeleteTopics)
	t.Run("Produce", testProduce)
	t.Run("Consume", testConsume)
	t.Run("Offsets", testOffsets)
	t.Run("WatchTopic", testWatchTopics)
	t.Run("Lock", testLock)

	t.Run("Options", testOptions)
}

func testNewClient(t *testing.T) {

	t.Run("New client w/valid option", func(t *testing.T) {
		c, err := NewClient(WithTimeout(time.Second * 2))
		defer c.Close()
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("New client w/no options", func(t *testing.T) {
		c, err := NewClient()
		defer c.Close()
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

func testCreateTopic(t *testing.T) {
	t.Run("CreateTopic", func(t *testing.T) {
		c, err := NewClient(WithTimeout(time.Second * 1))
		if err != nil {
			t.Fatal(err)
		}

		err = c.CreateTopic(context.Background(), []byte("test-topic"))
		if err != nil && err != ErrTopicExists {
			t.Fatal(err)
		}
	})
	t.Run("CreateTopic again", func(t *testing.T) {
		c, err := NewClient(WithTimeout(time.Second * 1))
		if err != nil {
			t.Fatal(err)
		}

		err = c.CreateTopic(context.Background(), []byte("test-topic"))
		if err != ErrTopicExists {
			t.Fatal(err)
		}
	})
}

func testListTopics(t *testing.T) {
	t.Run("ListTopics", func(t *testing.T) {
		c, err := NewClient(WithTimeout(time.Second * 1))
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
	})
	t.Run("ListTopics w/regex", func(t *testing.T) {
		c, err := NewClient(WithTimeout(time.Second * 1))
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		topics, err := c.ListTopics(ctx, "", "", ".*")
		if err != nil {
			t.Fatal(err)
		}
		if len(topics) == 0 {
			t.Fatal("missing topics")
		}
	})
	t.Run("ListTopics w/invalid regex", func(t *testing.T) {
		c, err := NewClient(WithTimeout(time.Second * 1))
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		_, err = c.ListTopics(ctx, "", "", `\`)
		if err == nil {
			t.Fatal("expected invalid regex err")
		}
	})
}

func testDeleteTopics(t *testing.T) {
	t.Run("Delete Listed Topics", func(t *testing.T) {
		c, err := NewClient(WithTimeout(time.Second * 1))
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
		for _, topic := range topics {
			err = c.DeleteTopic(ctx, topic)
			if err != nil {
				t.Fatal(err)
			}
		}
	})
}

func testProduce(t *testing.T) {
	t.Run("Invalid Produce Loop", func(t *testing.T) {
		c, err := NewClient(WithTimeout(time.Second * 1))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var ch chan ProduceMsg
		err = c.ProduceLoop(ctx, []byte("test-topic"), ch)
		if err.Error() != "invalid channel capacity, channels must have a capacity of at least 1" {
			t.Fatal(err)
		}
		ch = make(chan ProduceMsg, 0)
		err = c.ProduceLoop(ctx, []byte("test-topic"), ch)
		if err.Error() != "invalid channel capacity, channels must have a capacity of at least 1" {
			t.Fatal(err)
		}
	})
	t.Run("Produce Loop", func(t *testing.T) {
		c, err := NewClient(WithTimeout(time.Second * 1))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ch := make(chan ProduceMsg, 1)
		go c.ProduceLoop(ctx, []byte("test-topic"), ch)

		msg := NewProduceMsg([]byte("hello world"))
		ch <- msg
		err = <-msg.Err
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("Produce Loop w/ invalid preprocess", func(t *testing.T) {
		c, err := NewClient(WithTimeout(time.Second * 1))
		if err != nil {
			t.Fatal(err)
		}
		c.preProcess = []func(msgs [][]byte) error{func(msgs [][]byte) error {
			return errMock
		}}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ch := make(chan ProduceMsg, 1)
		go c.ProduceLoop(ctx, []byte("test-topic"), ch)

		msg := NewProduceMsg([]byte("hello world"))
		ch <- msg
		err = <-msg.Err
		if err != errMock {
			t.Fatal(err)
		}
	})
	t.Run("Produce 0 messages", func(t *testing.T) {
		c, err := NewClient(WithTimeout(time.Second * 1))
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		err = c.Produce(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("Produce message with invalid preprocess", func(t *testing.T) {
		c, err := NewClient(WithTimeout(time.Second * 1))
		if err != nil {
			t.Fatal(err)
		}
		c.preProcess = []func(msgs [][]byte) error{func(msgs [][]byte) error {
			return errMock
		}}
		ctx := context.Background()
		err = c.Produce(ctx, nil, []byte("message"))
		if err != errMock {
			t.Fatal(err)
		}
	})
}

func testConsume(t *testing.T) {
	t.Run("Consume", func(t *testing.T) {
		c, err := NewClient(WithTimeout(time.Second * 1))
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		topic := []byte("consumeTopic")
		msg := []byte("consume message")
		err = c.Produce(ctx, topic, msg)
		if err != nil {
			t.Fatal(err)
		}

		msgs, err := c.Consume(ctx, topic, 0, 1, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) != 1 || !bytes.Equal(msgs[0], msg) {
			t.Fatal(msgs)
		}
	})
}

func testOffsets(t *testing.T) {
	t.Run("Check offsets", func(t *testing.T) {
		c, err := NewClient(WithTimeout(time.Second * 1))
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		topic := []byte("offsets_topic")
		err = c.Produce(context.Background(), topic, []byte("single message"))
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
	})
	t.Run("Check missing offsets", func(t *testing.T) {
		c, err := NewClient(WithTimeout(time.Second * 1))
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		topic := []byte("missing_offsets_topic")
		_, _, err = c.Offsets(ctx, topic)
		if err != ErrTopicDoesNotExist {
			t.Fatal(err)
		}
	})
}

func testWatchTopics(t *testing.T) {
	t.Run("invalid topics", func(t *testing.T) {
		c, err := NewClient(WithTimeout(time.Second * 1))
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		ch := make(chan WatchEvent, 1)
		err = c.WatchTopics(ctx, ch)
		if err.Error() != "invalid number of topics sent" {
			t.Fatal(err)
		}
	})
	t.Run("missing topic", func(t *testing.T) {
		c, err := NewClient(WithTimeout(time.Second * 1))
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		ch := make(chan WatchEvent, 1)
		err = c.WatchTopics(ctx, ch, []byte("missingWatchTopic"))
		if err == nil {
			t.Fatal("expected to pass")
		}
	})
	t.Run("valid topic", func(t *testing.T) {
		c, err := NewClient(WithTimeout(time.Second * 1))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		topic := []byte("watchTopic")
		err = c.CreateTopic(ctx, topic)
		if err != nil && err != ErrTopicExists {
			t.Fatal(err)
		}
		ch := make(chan WatchEvent, 1)

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
	})
}

func testLock(t *testing.T) {
	t.Run("valid lock", func(t *testing.T) {
		c, err := NewClient(WithTimeout(time.Second * 1))
		if err != nil {
			t.Fatal(err)
		}
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
	})
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
