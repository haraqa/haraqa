package testing

import (
	"context"
	"crypto/rand"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/haraqa/haraqa"
	"github.com/haraqa/haraqa/broker"
)

func BenchmarkProduce(b *testing.B) {
	defer os.RemoveAll(".haraqa")
	cfg := broker.DefaultConfig
	b.Run("file queue", benchProducer(cfg))
	b.Run("file queue stream", benchProducerStream(cfg))

	mockQueue := broker.NewMockQueue(gomock.NewController(b))
	mockQueue.EXPECT().CreateTopic(gomock.Any()).Return(nil).AnyTimes()
	mockQueue.EXPECT().Produce(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(tcpConn *os.File, topic []byte, msgSizes []int64) error {
			var n int64
			for i := range msgSizes {
				n += msgSizes[i]
			}
			io.CopyN(ioutil.Discard, tcpConn, n)
			return nil
		}).AnyTimes()

	cfg.Queue = mockQueue
	b.Run("mock queue", benchProducer(cfg))
	b.Run("mock queue stream", benchProducerStream(cfg))
}

func benchProducer(cfg broker.Config) func(b *testing.B) {
	return func(b *testing.B) {
		brkr, err := broker.NewBroker(cfg)
		if err != nil {
			b.Fatal(err)
		}
		go func() {
			err := brkr.Listen(":4353", ":14353")
			if err != nil {
				b.Fatal(err)
			}
		}()
		defer brkr.Close()

		client, err := haraqa.NewClient(haraqa.DefaultConfig)
		if err != nil {
			b.Fatal(err)
		}
		ctx := context.Background()
		topic := []byte("something")
		client.CreateTopic(ctx, topic)

		batchSize := 2
		msgs := make([][]byte, batchSize)
		for i := range msgs {
			msgs[i] = make([]byte, 100)
			rand.Read(msgs[i])
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i += batchSize {
			err := client.Produce(ctx, topic, msgs...)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	}
}

func benchProducerStream(cfg broker.Config) func(b *testing.B) {
	return func(b *testing.B) {
		brkr, err := broker.NewBroker(cfg)
		if err != nil {
			b.Fatal(err)
		}
		go func() {
			err := brkr.Listen(":4353", ":14353")
			if err != nil {
				b.Fatal(err)
			}
		}()
		defer brkr.Close()

		client, err := haraqa.NewClient(haraqa.DefaultConfig)
		if err != nil {
			b.Fatal(err)
		}
		ctx := context.Background()
		topic := []byte("something")
		client.CreateTopic(ctx, topic)

		batchSize := 2000
		msg := make([]byte, 100)
		rand.Read(msg)

		ch := make(chan haraqa.ProduceMsg, batchSize)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			err = client.ProduceStream(ctx, topic, ch)
			if err != nil {
				b.Fatal(err)
			}
			wg.Done()
		}()

		errs := make([]chan error, b.N)
		for i := range errs {
			errs[i] = make(chan error, 1)
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			ch <- haraqa.ProduceMsg{
				Msg: msg,
				Err: errs[i],
			}
		}
		close(ch)
		wg.Wait()

		b.StopTimer()
	}
}
