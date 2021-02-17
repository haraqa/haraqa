package benchmarks

import (
	"bytes"
	"fmt"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/haraqa/haraqa"
	"github.com/haraqa/haraqa/pkg/server"
)

func BenchmarkConsume(b *testing.B) {
	b.SkipNow()

	var err error
	dirNames := make([]string, 1)
	for i := range dirNames {
		dirNames[i], err = os.MkdirTemp("", ".haraqa*")
		if err != nil {
			b.Error(err)
		}
	}
	defer func() {
		for _, dirName := range dirNames {
			if err := os.RemoveAll(dirName); err != nil {
				b.Error(err)
			}
		}
	}()
	haraqaServer, err := server.NewServer(
		server.WithFileQueue(dirNames, true, 5000),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer haraqaServer.Close()

	s := httptest.NewServer(haraqaServer)
	s.EnableHTTP2 = true
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
	fmt.Println("")
}

func benchConsumer(batchSize int, c *haraqa.Client) func(b *testing.B) {
	buf := new(bytes.Buffer)
	return func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; {
			r, _, err := c.Consume("benchtopic", 0, batchSize)
			if err != nil {
				b.Fatal(err)
			}
			buf.Reset()
			n, err := buf.ReadFrom(r)
			if err != nil {
				b.Fatal(err)
			}
			r.Close()

			i += int(n) / 100
		}
		b.StopTimer()
	}
}

func benchConsumerN(N, batchSize int, c *haraqa.Client) func(b *testing.B) {
	return func(b *testing.B) {
		var wg sync.WaitGroup
		ch := make(chan struct{}, N)
		defer close(ch)
		var failures int64
		for i := 0; i < N; i++ {
			go func() {
				buf := new(bytes.Buffer)
				for range ch {
					var N int
					for N < batchSize {
						r, sizes, err := c.Consume("benchtopic", 0, batchSize-N)
						if err != nil {
							b.Fatal(err)
						}
						if len(sizes) == 0 {
							continue
						}
						buf.Reset()
						n, err := buf.ReadFrom(r)
						if err != nil {
							b.Log(err, n, sizes)
							fmt.Println(err, n, sizes)
							atomic.AddInt64(&failures, 1)
						}
						r.Close()
						N += int(n) / 100
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
		if failures > 0 {
			b.Log(failures)
			b.Fail()
		}
	}
}
