package benchmarks

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/haraqa/haraqa/pkg/haraqa"
	"github.com/haraqa/haraqa/pkg/server"
)

func BenchmarkConsume(b *testing.B) {
	rnd := make([]byte, 12)
	rand.Read(rnd)
	randomName := base64.URLEncoding.EncodeToString(rnd)

	dirNames := []string{
		".haraqa1-" + randomName,
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
		b.Fatal(err)
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
		b.Fatal(err)
	}

	msgs := make([][100]byte, 1000)
	sizes := make([]int64, len(msgs))
	var data []byte
	for i := range msgs {
		copy(msgs[i][:], []byte("something"))
		data = append(data, msgs[i][:]...)
		sizes[i] = int64(len(msgs[i]))
	}

	for i := 0; i < b.N; i += len(msgs) {
		body := bytes.NewBuffer(data)
		err = c.Produce("benchtopic", sizes, body)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.Run("consume 1", benchConsumer(1, c))
	b.Run("consume 10", benchConsumer(10, c))
	b.Run("consume 100", benchConsumer(100, c))
	b.Run("consume 1000", benchConsumer(1000, c))
	fmt.Println("")
	b.Run("go consume 1", benchConsumerN(10, 1, c))
	b.Run("go consume 10", benchConsumerN(10, 10, c))
	b.Run("go consume 100", benchConsumerN(10, 100, c))
	b.Run("go consume 1000", benchConsumerN(10, 1000, c))
}

func benchConsumer(batchSize int, c *haraqa.Client) func(b *testing.B) {
	return func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; {
			r, _, err := c.Consume("benchtopic", 0, batchSize)
			if err != nil {
				b.Fatal(err)
			}
			body, err := ioutil.ReadAll(r)
			if err != nil {
				b.Fatal(err)
			}
			r.Close()

			i += len(body) / 100
		}
		b.StopTimer()
	}
}

func benchConsumerN(N, batchSize int, c *haraqa.Client) func(b *testing.B) {
	return func(b *testing.B) {
		var wg sync.WaitGroup
		ch := make(chan struct{}, N)
		defer close(ch)
		for i := 0; i < N; i++ {
			go func() {
				for range ch {
					var n int
					for n < batchSize {
						r, _, err := c.Consume("benchtopic", 0, batchSize-n)
						if err != nil {
							b.Fatal(err)
						}
						body, err := ioutil.ReadAll(r)
						if err != nil {
							b.Fatal(err)
						}
						r.Close()
						n += len(body) / 100
					}
					wg.Done()
				}
			}()
		}
		b.ReportAllocs()
		for i := 0; i < b.N; i += batchSize {
			wg.Add(1)
			ch <- struct{}{}
		}
		wg.Wait()
		b.StopTimer()
	}
}
