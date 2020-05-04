package testing

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log"
	"math/big"
	"os"
	"strconv"
	"testing"

	"github.com/haraqa/haraqa"
	"github.com/haraqa/haraqa/broker"
	"github.com/haraqa/haraqa/internal/protocol"
	"google.golang.org/grpc"
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
	fmt.Println("")
	b.Run("consume grpc 1", benchGRPCConsumer(1, msgs))
	b.Run("consume grpc 10", benchGRPCConsumer(10, msgs))
	b.Run("consume grpc 100", benchGRPCConsumer(100, msgs))
	b.Run("consume grpc 1000", benchGRPCConsumer(1000, msgs))
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

var grpcConsumeRespDump *protocol.GRPCConsumeResponse

func benchGRPCConsumer(batchSize int, msgs [][]byte) func(b *testing.B) {
	return func(b *testing.B) {
		ctx := context.Background()
		grpcConn, err := grpc.DialContext(ctx, haraqa.DefaultAddr+":"+strconv.Itoa(haraqa.DefaultGRPCPort), grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			b.Fatal(err)
		}
		defer grpcConn.Close()
		grpcClient := protocol.NewHaraqaClient(grpcConn)

		topic := []byte("consumable")
		req := &protocol.GRPCConsumeRequest{
			Topic:  topic,
			Offset: 0,
			Limit:  int64(batchSize),
		}
		output := make([][]byte, 0, len(msgs))
		var n int64
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i += batchSize {
			n = 0
			resp, err := grpcClient.Consume(ctx, req)
			if err != nil {
				b.Fatal(err)
			}
			if !resp.GetMeta().GetOK() {
				b.Fatal(resp.GetMeta())
			}
			req.Offset += int64(len(resp.GetMsgSizes()))
			buf := resp.GetMessages()
			for _, size := range resp.GetMsgSizes() {
				output = append(output, buf[n:n+size])
				n += size
			}
		}
		b.StopTimer()
		for i := range output {
			if !bytes.Equal(msgs[i], output[i]) {
				b.Log("mismatch at index", i, "\n", string(msgs[i]), "\n", string(output[i]))
				b.Fail()
			}
		}
	}
}
