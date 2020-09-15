package benchmarks

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/haraqa/haraqa/pkg/haraqa"
	"github.com/haraqa/haraqa/pkg/server"
)

func BenchmarkProduce(b *testing.B) {
	defer os.RemoveAll(".haraqa")

	rnd := make([]byte, 10)
	rand.Read(rnd)
	randomName := base64.URLEncoding.EncodeToString(rnd)
	dirNames := []string{
		".haraqa1-" + randomName,
		".haraqa2-" + randomName,
	}
	defer func() {
		for _, name := range dirNames {
			os.RemoveAll(name)
		}
	}()

	haraqaServer, err := server.NewServer(
		server.WithDirs(dirNames...),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer haraqaServer.Close()

	s := httptest.NewServer(haraqaServer)
	defer s.Close()

	c, err := haraqa.NewClient(haraqa.WithURL(s.URL))
	if err != nil {
		b.Fatal(err)
	}
	err = c.CreateTopic("benchtopic")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("")
	b.Run("produce 1", benchProducer(1, c))
	b.Run("produce 10", benchProducer(10, c))
	b.Run("produce 100", benchProducer(100, c))
	b.Run("produce 1000", benchProducer(1000, c))
	fmt.Println("")
	b.Run("go produce 1", benchProducerN(10, 1, c))
	b.Run("go produce 10", benchProducerN(10, 10, c))
	b.Run("go produce 100", benchProducerN(10, 100, c))
	b.Run("go produce 1000", benchProducerN(10, 1000, c))
}

func benchProducer(batchSize int, c *haraqa.Client) func(b *testing.B) {
	var err error
	msgs := make([][100]byte, batchSize)
	sizes := make([]int64, len(msgs))
	var data []byte
	for i := range msgs {
		copy(msgs[i][:], []byte("something"))
		data = append(data, msgs[i][:]...)
		sizes[i] = int64(len(msgs[i]))
	}
	return func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i += len(msgs) {
			body := bytes.NewBuffer(data)
			err = c.Produce("benchtopic", sizes, body)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	}
}

func benchProducerN(N, batchSize int, c *haraqa.Client) func(b *testing.B) {
	var err error
	msgs := make([][100]byte, batchSize)
	sizes := make([]int64, len(msgs))
	var data []byte
	for i := range msgs {
		copy(msgs[i][:], []byte("something"))
		data = append(data, msgs[i][:]...)
		sizes[i] = int64(len(msgs[i]))
	}
	return func(b *testing.B) {
		ch := make(chan struct{}, N)
		defer close(ch)
		var wg sync.WaitGroup
		for i := 0; i < N; i++ {
			go func() {
				for range ch {
					body := bytes.NewBuffer(data)
					err = c.Produce("benchtopic", sizes, body)
					if err != nil {
						b.Fatal(err)
					}
					wg.Done()
				}
			}()
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i += len(msgs) {
			wg.Add(1)
			ch <- struct{}{}
		}
		wg.Wait()
		b.StopTimer()
	}
}
