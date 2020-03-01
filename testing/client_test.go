package testing

import (
	"bytes"
	"context"
	"io"
	"os"
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
			t.Log(err)
			t.Fail()
		}
	}()
	defer b.Close()

	t.Run("lock", testLock)
	t.Run("producer", testProduce)
	t.Run("consumer", testConsumer)
	wg.Wait()
}

func testProduce(t *testing.T) {
	client, err := haraqa.NewClient(haraqa.WithUnixSocket("/tmp/haraqa.sock"))
	for err != nil {
		client, err = haraqa.NewClient(haraqa.WithUnixSocket("/tmp/haraqa.sock"))
	}
	ctx := context.Background()
	err = client.Produce(ctx, []byte("world"), []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
}

func testConsumer(t *testing.T) {
	client, err := haraqa.NewClient(haraqa.WithTimeout(time.Second * 1))
	for err != nil {
		t.Log(err)
		client, err = haraqa.NewClient(haraqa.WithTimeout(time.Second * 1))
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

func TestAes(t *testing.T) {
	b, err := broker.NewBroker(broker.DefaultConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	go func() {
		err := b.Listen()
		if err != nil {
			t.Log(err)
			t.Fail()
		}
	}()

	aesKey := [32]byte{}
	copy(aesKey[:], []byte("SECRET_KEY"))

	client, err := haraqa.NewClient(haraqa.WithAESGCM(aesKey))
	for err != nil {
		t.Log(err)
		client, err = haraqa.NewClient(haraqa.WithAESGCM(aesKey))
	}
	ctx := context.Background()
	topic := []byte("aes-topic")
	msgs := [][]byte{[]byte("hello"), []byte("world")}
	err = client.Produce(ctx, topic, msgs...)
	if err != nil {
		t.Fatal(err)
	}

	consumedMsgs, err := client.Consume(ctx, topic, 0, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(consumedMsgs[0]) != "hello" || string(consumedMsgs[1]) != "world" {
		t.Fatal(consumedMsgs)
	}
}

func TestWatcher(t *testing.T) {
	config := broker.DefaultConfig
	config.UnixSocket = "/tmp/haraqa.sock"
	config.Volumes = []string{"watcherVol"}
	_ = os.Mkdir("watcherVol", 0777)
	b, err := broker.NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	go func() {
		err := b.Listen()
		if err != nil {
			t.Log(err)
			t.Fail()
		}
	}()
	client, err := haraqa.NewClient()
	for err != nil {
		t.Log(err)
		client, err = haraqa.NewClient()
	}
	ctx := context.Background()

	// create topics
	topic1, topic2 := []byte("topic1"), []byte("topic2")
	_ = client.CreateTopic(ctx, topic1)
	_ = client.CreateTopic(ctx, topic2)

	// start watcher
	watchEvents := make(chan haraqa.WatchEvent, 1)
	ctxCancel, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		err = client.WatchTopics(ctxCancel, watchEvents, topic1, topic2)
		if err != nil && err != ctxCancel.Err() {
			t.Log(err)
			t.Fail()
		}
	}()

	// wait a moment so goroutine can establish watch
	time.Sleep(time.Millisecond * 20)

	// produce to topic 1
	_ = client.Produce(ctx, topic1, []byte("hello world"))

	// handle topic 1 event
	event := <-watchEvents
	if event.MinOffset != 0 || event.MaxOffset != 1 || !bytes.Equal(event.Topic, topic1) {
		t.Fatal(event)
	}

	// produce to topic 2
	_ = client.Produce(ctx, topic2, []byte("hello there"), []byte("hello again"))
	event = <-watchEvents
	if event.MinOffset != 0 || event.MaxOffset != 2 || !bytes.Equal(event.Topic, topic2) {
		t.Fatal(event)
	}
}

func testLock(t *testing.T) {
	client1, err := haraqa.NewClient(haraqa.WithTimeout(time.Second * 1))
	for err != nil {
		t.Log(err)
		client1, err = haraqa.NewClient(haraqa.WithTimeout(time.Second * 1))
	}
	defer client1.Close()

	client2, err := haraqa.NewClient(haraqa.WithTimeout(time.Second * 1))
	for err != nil {
		t.Log(err)
		client2, err = haraqa.NewClient(haraqa.WithTimeout(time.Second * 1))
	}
	defer client2.Close()

	lock1, locked, err := client1.Lock(context.Background(), []byte("group"), true)
	if err != nil {
		t.Fatal(err)
	}
	if !locked {
		t.Fatal("failed to lock client 1")
	}

	before := time.Now()
	go func() {
		time.Sleep(time.Second * 3)
		err := lock1.Close()
		if err != nil {
			t.Log(err)
			t.Fail()
		}
	}()

	lock2, locked, err := client2.Lock(context.Background(), []byte("group"), true)
	if err != nil {
		t.Fatal(err)
	}
	lock2.Close()
	if !locked {
		t.Fatal("failed to lock client 2")
	}
	after := time.Now()

	if after.Sub(before) < time.Second*3 {
		t.Fatal("did not hold lock long enough")
	}
}
