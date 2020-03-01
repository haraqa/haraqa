package testing

import (
	"context"
	"crypto/rand"
	"log"
	"math/big"
	"os"
	"testing"

	"github.com/haraqa/haraqa"
	"github.com/haraqa/haraqa/broker"
)

func BenchmarkConsume(b *testing.B) {
	defer os.RemoveAll(".haraqa")
	cfg := broker.DefaultConfig
	brkr, err := broker.NewBroker(cfg)
	if err != nil {
		b.Fatal(err)
	}
	go func() {
		err := brkr.Listen()
		if err != nil {
			b.Log(err)
			b.Fail()
		}
	}()
	createConsumeTopic()
	b.Run("consume 1", benchConsumer(1))
	b.Run("consume 10", benchConsumer(10))
	b.Run("consume 100", benchConsumer(100))
	b.Run("consume 1000", benchConsumer(1000))

	brkr.Close()
}

func createConsumeTopic() {
	client, err := haraqa.NewClient()
	if err != nil {
		log.Fatal(err)
	}
	topic := []byte("consumable")
	_ = client.CreateTopic(context.Background(), topic)

	msgs := make([][]byte, 1000)
	for i := range msgs {
		n, _ := rand.Int(rand.Reader, big.NewInt(20))
		msgs[i] = make([]byte, n.Int64()+90)
		_, _ = rand.Read(msgs[i])
	}

	err = client.Produce(context.Background(), topic, msgs...)
	if err != nil {
		panic(err)
	}

	client.Close()
}

func benchConsumer(batchSize int) func(b *testing.B) {
	return func(b *testing.B) {
		ctx := context.Background()

		client, err := haraqa.NewClient()
		if err != nil {
			b.Fatal(err)
		}
		topic := []byte("consumable")

		var offset int64
		buf := haraqa.NewConsumeBuffer()
		b.ReportAllocs()
		b.ResetTimer()
		for offset < int64(b.N) {
			discardBatch, err = client.Consume(ctx, topic, offset, int64(batchSize), buf)
			if err != nil {
				b.Fatal(err)
			}
			offset += int64(len(discardBatch))
		}

		b.StopTimer()
	}
}

var discardBatch [][]byte
