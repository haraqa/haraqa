package testing

import (
	"context"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/haraqa/haraqa"
	"github.com/haraqa/haraqa/broker"
)

func TestClient(t *testing.T) {

	var wg sync.WaitGroup
	wg.Add(2)
	mockQueue := broker.NewMockQueue(gomock.NewController(t))
	mockQueue.EXPECT().Produce(gomock.Any(), []byte("world"), []int64{5}).
		DoAndReturn(func(tcpConn *os.File, topic []byte, msgSizes []int64) error {
			var b [5]byte
			_, err := io.ReadFull(tcpConn, b[:])
			if err != nil {
				t.Fatal(err)
			}
			if string(b[:]) != "hello" {
				t.Fatal(string(b[:]))
			}
			wg.Done()
			return nil
		})
	mockQueue.EXPECT().ConsumeInfo([]byte("world"), int64(20), int64(10)).
		Return([]byte("filename"), int64(100), []int64{5, 6, 9}, nil)

	mockQueue.EXPECT().Consume(gomock.Any(), []byte("world"), []byte("filename"), int64(100), int64(20)).
		DoAndReturn(func(tcpConn *os.File, topic, filename []byte, startAt, totalSize int64) error {

			_, err := tcpConn.Write([]byte("hello there consumer"))
			if err != nil {
				t.Fatal(err)
			}
			wg.Done()
			return nil
		})

	cfg := broker.DefaultConfig
	cfg.Queue = mockQueue
	b, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		err := b.Listen()
		if err != nil {
			t.Fatal(err)
		}
	}()
	defer b.Close()

	t.Run("producer", testProduce)
	t.Run("consumer", testConsumer)
	wg.Wait()
}

func testProduce(t *testing.T) {
	config := haraqa.DefaultConfig
	config.UnixSocket = "/tmp/haraqa.sock"

	client, err := haraqa.NewClient(config)
	for err != nil {
		client, err = haraqa.NewClient(config)
	}
	ctx := context.Background()
	err = client.Produce(ctx, []byte("world"), []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
}

func testConsumer(t *testing.T) {
	config := haraqa.DefaultConfig
	config.Timeout = time.Second * 1
	client, err := haraqa.NewClient(config)
	for err != nil {
		t.Log(err)
		client, err = haraqa.NewClient(config)
	}
	ctx := context.Background()
	msgs, err := client.Consume(ctx, []byte("world"), 20, 10, nil)
	if err != nil {
		t.Fatal(err)
	}

	if string(msgs[0]) != "hello" {
		t.Fatal(string(msgs[0]))
	}
	if string(msgs[1]) != " there" {
		t.Fatal(string(msgs[1]))
	}
	if string(msgs[2]) != " consumer" {
		t.Fatal(string(msgs[2]))
	}
}
