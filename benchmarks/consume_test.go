package benchmarks

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"io/ioutil"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/haraqa/haraqa/client"
	"github.com/haraqa/haraqa/server"
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

	c := client.NewClient(s.URL)

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
}

func benchConsumer(batchSize int, c *client.Client) func(b *testing.B) {
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
