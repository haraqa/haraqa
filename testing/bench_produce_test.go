package testing

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/haraqa/haraqa"
	"github.com/haraqa/haraqa/broker"
)

var errDump error

func BenchmarkProduce(b *testing.B) {
	defer os.RemoveAll(".haraqa")
	brkr, err := broker.NewBroker()
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		err := brkr.Listen(ctx)
		if err != nil && err != ctx.Err() {
			b.Log(err)
			b.Fail()
		}
	}()
	fmt.Println("")
	b.Run("produce 1", benchProducer(1))
	b.Run("produce 10", benchProducer(10))
	b.Run("produce 100", benchProducer(100))
	b.Run("produce 1000", benchProducer(1000))
	fmt.Println("")
	b.Run("produce loop 1", benchProducerLoop(1))
	b.Run("produce loop 10", benchProducerLoop(10))
	b.Run("produce loop 100", benchProducerLoop(100))
	b.Run("produce loop 1000", benchProducerLoop(1000))
}

func benchProducer(batchSize int) func(b *testing.B) {
	return func(b *testing.B) {
		client, err := haraqa.NewClient()
		if err != nil {
			b.Fatal(err)
		}
		ctx := context.Background()
		topic := []byte("something")
		_ = client.CreateTopic(ctx, topic)

		msgs := make([][]byte, batchSize)
		for i := range msgs {
			msgs[i] = make([]byte, 100)
			_, _ = rand.Read(msgs[i])
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i += batchSize {
			errDump = client.Produce(ctx, topic, msgs...)
		}
		b.StopTimer()
	}
}

func benchProducerLoop(batchSize int) func(b *testing.B) {
	return func(b *testing.B) {
		client, err := haraqa.NewClient()
		if err != nil {
			b.Fatal(err)
		}
		ctx := context.Background()
		topic := []byte("something")
		_ = client.CreateTopic(ctx, topic)

		msg := make([]byte, 100)
		_, _ = rand.Read(msg)

		producer, err := client.NewProducer(haraqa.WithTopic(topic), haraqa.WithBatchSize(batchSize))
		if err != nil {
			b.Fatal(err)
		}
		defer producer.Close()

		// create n workers
		var wg sync.WaitGroup
		blocker := make(chan struct{})
		for i := 0; i < batchSize; i++ {
			wg.Add(1)
			go func() {
				<-blocker
				for j := 0; j < b.N; j += batchSize {
					errDump = producer.Send(msg)
				}
				wg.Done()
			}()
		}

		b.ReportAllocs()
		b.ResetTimer()

		close(blocker)
		wg.Wait()
		b.StopTimer()
	}
}
