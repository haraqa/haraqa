package benchmarks

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/haraqa/haraqa/client"
	"github.com/haraqa/haraqa/server"
)

func BenchmarkProduce(b *testing.B) {
	defer os.RemoveAll(".haraqa")

	fmt.Println("")
	b.Run("produce 1", benchProducer(1))
	b.Run("produce 10", benchProducer(10))
	b.Run("produce 100", benchProducer(100))
	b.Run("produce 1000", benchProducer(1000))
}

func benchProducer(batchSize int) func(b *testing.B) {
	return func(b *testing.B) {
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

		msgs := make([][100]byte, batchSize)
		sizes := make([]int64, len(msgs))
		var data []byte
		for i := range msgs {
			copy(msgs[i][:], []byte("something"))
			data = append(data, msgs[i][:]...)
			sizes[i] = int64(len(msgs[i]))
		}

		b.ReportAllocs()
		b.ResetTimer()
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
