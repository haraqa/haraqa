package testing

import (
	"context"
	"crypto/rand"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
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
			b.Fatal(err)
		}
	}()
	b.Run("file queue next 1", benchConsumer(1, true))
	b.Run("file queue next 2", benchConsumer(2, true))
	b.Run("file queue next 5", benchConsumer(5, true))

	b.Run("file queue batch 1", benchConsumer(1, false))
	b.Run("file queue batch 2", benchConsumer(2, false))
	b.Run("file queue batch 5", benchConsumer(5, false))

	brkr.Close()
}

func benchConsumer(num int, next bool) func(b *testing.B) {
	{
		client, err := haraqa.NewClient(haraqa.DefaultConfig)
		if err != nil {
			log.Fatal(err)
		}
		for i := 0; i < num; i++ {
			topic := []byte("consumable" + strconv.Itoa(i))
			client.CreateTopic(context.Background(), topic)
		}
		client.Close()
	}

	return func(b *testing.B) {
		ch := make(chan struct{})
		errs := make(chan error, b.N/num)
		var wg1, wg2 sync.WaitGroup
		wg1.Add(num)
		wg2.Add(num)
		for v := 0; v < num; v++ {
			go func(v int) {
				ctx := context.Background()
				defer wg2.Done()

				client, err := haraqa.NewClient(haraqa.DefaultConfig)
				if err != nil {
					errs <- err
					wg1.Done()
					return
				}
				topic := []byte("consumable" + strconv.Itoa(v))

				batchSize := 200
				msgs := make([][]byte, batchSize)
				for i := range msgs {
					msgs[i] = make([]byte, 100)
					_, err = rand.Read(msgs[i])
					if err != nil {
						errs <- err
						wg1.Done()
						return
					}
				}

				for i := 0; i < b.N/num; i += batchSize {
					err := client.Produce(ctx, topic, msgs...)
					if err != nil {
						errs <- err
						wg1.Done()
						return
					}
				}

				var offset int64
				var resp haraqa.ConsumeResponse
				wg1.Done()
				<-ch
				for offset < int64(b.N/num) {
					err = client.Consume(ctx, topic, offset, int64(batchSize), &resp)
					if err != nil {
						errs <- err
						return
					}

					if next {
						for err == nil {
							discard, err = resp.Next()
							offset++
						}
						if err != io.EOF {
							errs <- err
							return
						}
					} else {
						db, err := resp.Batch()
						if err != nil {
							errs <- err
							return
						}
						discardBatch = db
						offset += int64(len(db))
					}

				}
			}(v)
		}

		wg1.Wait()
		select {
		case err := <-errs:
			b.Fatal(err)
		default:
		}

		b.ReportAllocs()
		b.ResetTimer()
		close(ch)
		wg2.Wait()
		b.StopTimer()
		select {
		case err := <-errs:
			b.Fatal(err)
		default:
		}
	}
}

var discard []byte
var discardBatch [][]byte
