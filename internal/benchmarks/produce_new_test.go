package benchmarks

import (
	"fmt"
	"log"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/haraqa/haraqa"
	"github.com/haraqa/haraqa/pkg/server"
)

func BenchmarkNewProduce(b *testing.B) {
	var err error
	dirNames := make([]string, 3)
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
		server.WithDefaultQueue(dirNames, true, 5000),
	)
	if err != nil {
		log.Fatal(err)
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
		log.Fatal(err)
	}

	b.Run("produce 1", benchProducer(1, c))
	b.Run("produce 10", benchProducer(10, c))
	b.Run("produce 100", benchProducer(100, c))
	b.Run("produce 1000", benchProducer(1000, c))
	fmt.Println("")
	b.Run("go produce 1", benchProducerN(10, 1, c))
	b.Run("go produce 10", benchProducerN(10, 10, c))
	b.Run("go produce 100", benchProducerN(10, 100, c))
	b.Run("go produce 1000", benchProducerN(10, 1000, c))
	fmt.Println("")
}
