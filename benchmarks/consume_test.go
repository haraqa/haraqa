package testing

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"log"
	"math/big"
	"os"
	"strconv"
	"testing"

	"github.com/haraqa/haraqa"
	"github.com/haraqa/haraqa/broker"
)

func BenchmarkConsume(b *testing.B) {
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
	msgs := createConsumeTopic()
	b.Run("consume 1", benchConsumer(1, msgs))
	b.Run("consume 10", benchConsumer(10, msgs))
	b.Run("consume 100", benchConsumer(100, msgs))
	b.Run("consume 1000", benchConsumer(1000, msgs))
}

func createConsumeTopic() [][]byte {
	client, err := haraqa.NewClient()
	if err != nil {
		log.Fatal(err)
	}
	topic := []byte("consumable")
	_ = client.CreateTopic(context.Background(), topic)

	msgs := make([][]byte, 1000)
	for i := range msgs {
		n, _ := rand.Int(rand.Reader, big.NewInt(20))
		data := make([]byte, n.Int64()+90)
		_, _ = rand.Read(data)
		msgs[i] = []byte("msg" + strconv.Itoa(i) + " " + base64.StdEncoding.EncodeToString(data) + "\n")
	}

	err = client.Produce(context.Background(), topic, msgs...)
	if err != nil {
		panic(err)
	}

	client.Close()
	return msgs
}

func benchConsumer(batchSize int, msgs [][]byte) func(b *testing.B) {
	return func(b *testing.B) {
		ctx := context.Background()

		client, err := haraqa.NewClient()
		if err != nil {
			b.Fatal(err)
		}
		topic := []byte("consumable")
		output := make([][]byte, 0, len(msgs))
		b.ReportAllocs()
		b.ResetTimer()
		for len(output) < len(msgs) {
			m, err := client.Consume(ctx, topic, int64(len(output)), int64(batchSize), nil)
			if err != nil {
				b.Fatal(err)
			}
			output = append(output, m...)
		}

		b.StopTimer()
		if len(msgs) != len(output) {
			b.Fatal(len(msgs), len(output))
		}
		for i := range msgs {
			if !bytes.Equal(msgs[i], output[i]) {
				b.Log("mismatch at index", i, "\n", string(msgs[i]), "\n", string(output[i]))
				b.Fail()
			}
		}
	}
}

var discardBatch [][]byte
