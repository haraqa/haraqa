package testing

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/haraqa/haraqa"
	"github.com/haraqa/haraqa/broker"
	"github.com/haraqa/haraqa/internal/queue"
)

func TestClient(t *testing.T) {

	var wg sync.WaitGroup
	wg.Add(2)
	mockQueue := queue.NewMockQueue(gomock.NewController(t))
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
	b, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b.Q = mockQueue
	go func() {
		err := b.Listen()
		if err != nil {
			t.Fatal(err)
		}
	}()
	defer b.Close()

	t.Run("lock", testLock)
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

func TestWatcher(t *testing.T) {
	config := broker.DefaultConfig
	config.UnixSocket = "/tmp/haraqa.sock"
	config.Volumes = []string{"watcherVol"}
	os.Mkdir("watcherVol", 0777)
	b, err := broker.NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	go func() {
		err := b.Listen()
		if err != nil {
			t.Fatal(err)
		}
	}()
	client, err := haraqa.NewClient(haraqa.DefaultConfig)
	for err != nil {
		t.Log(err)
		client, err = haraqa.NewClient(haraqa.DefaultConfig)
	}
	ctx := context.Background()

	// create topics
	topic1, topic2 := []byte("topic1"), []byte("topic2")
	err = client.CreateTopic(ctx, topic1)
	if err != nil {
		t.Fatal(err)
	}
	err = client.CreateTopic(ctx, topic2)
	if err != nil {
		t.Fatal(err)
	}

	dir, err := os.Open(filepath.Join(config.Volumes[0], string(topic2)))
	if err != nil {
		t.Fatal(err)
	}
	names, err := dir.Readdirnames(-1)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(names)
	// start watcher
	watchEvents := make(chan haraqa.WatchEvent, 1)
	closer, err := client.WatchTopics(ctx, watchEvents, topic1, topic2)
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()

	// produce to topic 1
	err = client.Produce(ctx, topic1, []byte("hello world"))
	if err != nil {
		t.Fatal(err)
	}

	// handle topic 1 event
	event := <-watchEvents
	if event.Err != nil {
		t.Fatal(event.Err)
	}
	if event.MinOffset != 0 || event.MaxOffset != 1 || !bytes.Equal(event.Topic, topic1) {
		t.Fatal(event)
	}

	// produce to topic 2
	err = client.Produce(ctx, topic2, []byte("hello there"), []byte("hello again"))
	if err != nil {
		t.Fatal(err)
	}
	event = <-watchEvents
	if event.Err != nil {
		t.Fatal(event.Err)
	}
	if event.MinOffset != 0 || event.MaxOffset != 2 || !bytes.Equal(event.Topic, topic2) {
		t.Fatal(event)
	}
}

func testLock(t *testing.T) {
	config := haraqa.DefaultConfig
	config.Timeout = time.Second * 1
	client1, err := haraqa.NewClient(config)
	for err != nil {
		t.Log(err)
		client1, err = haraqa.NewClient(config)
	}
	defer client1.Close()

	client2, err := haraqa.NewClient(config)
	for err != nil {
		t.Log(err)
		client2, err = haraqa.NewClient(config)
	}
	defer client2.Close()

	lock1, err := client1.Lock(context.Background(), []byte("group"))
	if err != nil {
		t.Fatal(err)
	}

	before := time.Now()
	go func() {
		time.Sleep(time.Second * 3)
		err := lock1.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	lock2, err := client2.Lock(context.Background(), []byte("group"))
	if err != nil {
		t.Fatal(err)
	}
	lock2.Close()
	after := time.Now()

	if after.Sub(before) < time.Second*3 {
		t.Fatal("did not hold lock long enough")
	}
}
