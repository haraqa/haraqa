package testing

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/haraqa/haraqa"
	"github.com/haraqa/haraqa/broker"
	"github.com/haraqa/haraqa/internal/protocol"
	"google.golang.org/grpc"
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
	b.Run("producer 1", benchProducerSend(1))
	b.Run("producer 10", benchProducerSend(10))
	b.Run("producer 100", benchProducerSend(100))
	b.Run("producer 1000", benchProducerSend(1000))
	fmt.Println("")
	b.Run("producer ch 1", benchProducerErrChan(1))
	b.Run("producer ch 10", benchProducerErrChan(10))
	b.Run("producer ch 100", benchProducerErrChan(100))
	b.Run("producer ch 1000", benchProducerErrChan(1000))
	fmt.Println("")
	b.Run("produce grpc 1", benchGRPCProducer(1))
	b.Run("produce grpc 10", benchGRPCProducer(10))
	b.Run("produce  grpc 100", benchGRPCProducer(100))
	b.Run("produce grpc 1000", benchGRPCProducer(1000))
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

func benchProducerSend(batchSize int) func(b *testing.B) {
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

func benchProducerErrChan(batchSize int) func(b *testing.B) {
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

		errs := make(chan error, b.N+1)

		producer, err := client.NewProducer(
			haraqa.WithTopic(topic),
			haraqa.WithBatchSize(batchSize),
			haraqa.WithIgnoreErrors(),
			haraqa.WithErrorHandler(func(_ [][]byte, err error) {
				errs <- err
			}),
		)
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
					// ignoring errors, so Send will always be nil
					_ = producer.Send(msg)
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

var grpcProduceRespDump *protocol.GRPCProduceResponse

func benchGRPCProducer(batchSize int) func(b *testing.B) {
	return func(b *testing.B) {
		ctx := context.Background()
		grpcConn, err := grpc.DialContext(ctx, haraqa.DefaultAddr+":"+strconv.Itoa(haraqa.DefaultGRPCPort), grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			b.Fatal(err)
		}
		defer grpcConn.Close()
		grpcClient := protocol.NewHaraqaClient(grpcConn)

		topic := []byte("something")
		msgs := make([]byte, batchSize*100)
		_, _ = rand.Read(msgs)
		msgSizes := make([]int64, batchSize)
		for i := range msgSizes {
			msgSizes[i] = 100
		}

		req := &protocol.GRPCProduceRequest{
			Topic:    topic,
			MsgSizes: msgSizes,
			Messages: msgs,
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i += batchSize {
			grpcProduceRespDump, errDump = grpcClient.Produce(ctx, req)
		}
		b.StopTimer()
	}
}
