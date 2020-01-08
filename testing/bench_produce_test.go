package testing

import (
	"context"
	"crypto/rand"
	"os"
	"sync"
	"testing"

	"github.com/haraqa/haraqa"
	"github.com/haraqa/haraqa/broker"
)

func BenchmarkProduce(b *testing.B) {
	defer os.RemoveAll(".haraqa")
	cfg := broker.DefaultConfig
	b.Run("produce", benchProducer(cfg))
	b.Run("produce loop", benchProducerLoop(cfg))
}

func benchProducer(cfg broker.Config) func(b *testing.B) {
	return func(b *testing.B) {
		brkr, err := broker.NewBroker(cfg)
		if err != nil {
			b.Fatal(err)
		}
		go func() {
			err := brkr.Listen()
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

		batchSize := 10
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

func benchProducerLoop(cfg broker.Config) func(b *testing.B) {
	return func(b *testing.B) {
		brkr, err := broker.NewBroker(cfg)
		if err != nil {
			b.Fatal(err)
		}
		go func() {
			err := brkr.Listen()
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
			err := client.ProduceLoop(ctx, topic, ch)
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
