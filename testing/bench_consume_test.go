package testing

import (
	"context"
	"crypto/rand"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/haraqa/haraqa"
	"github.com/haraqa/haraqa/broker"
)

func BenchmarkConsume(b *testing.B) {
	defer os.RemoveAll(".haraqa")
	cfg := broker.DefaultConfig
	b.Run("file queue next 1", benchConsumer(cfg, 1, true))
	b.Run("file queue next 2", benchConsumer(cfg, 2, true))
	b.Run("file queue next 5", benchConsumer(cfg, 5, true))

	b.Run("file queue batch 1", benchConsumer(cfg, 1, false))
	b.Run("file queue batch 2", benchConsumer(cfg, 2, false))
	b.Run("file queue batch 5", benchConsumer(cfg, 5, false))

	mockQueue := broker.NewMockQueue(gomock.NewController(b))
	mockQueue.EXPECT().CreateTopic(gomock.Any()).Return(nil).AnyTimes()
	mockQueue.EXPECT().Produce(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(tcpConn *os.File, topic []byte, msgSizes []int64) error {
			var n int64
			for i := range msgSizes {
				n += msgSizes[i]
			}
			io.CopyN(ioutil.Discard, tcpConn, n)
			return nil
		}).AnyTimes()

	var msgSizes []int64
	mockQueue.EXPECT().ConsumeData(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(topic []byte, offset, maxBatchSize int64) ([]byte, int64, []int64, error) {
			if len(msgSizes) == 0 {
				msgSizes := make([]int64, maxBatchSize)
				for i := range msgSizes {
					msgSizes[i] = 100
				}
			}
			return nil, 0, msgSizes, nil
		}).Return(nil, int64(0), []int64{1}, nil).AnyTimes()

	var writeBuf []byte
	mockQueue.EXPECT().Consume(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(tcpConn *os.File, topic, filename []byte, startAt, totalSize int64) error {
			if int64(len(writeBuf)) < totalSize {
				writeBuf = make([]byte, totalSize)
			}
			tcpConn.Write(writeBuf[:totalSize])
			return nil
		}).AnyTimes()

	cfg.Queue = mockQueue
	b.Run("mock queue", benchConsumer(cfg, 1, true))
	//b.Run("mock queue stream", benchProducerStream(cfg))
}

func benchConsumer(cfg broker.Config, num int, next bool) func(b *testing.B) {
	return func(b *testing.B) {
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
		defer brkr.Close()

		ch := make(chan struct{})
		var wg1, wg2 sync.WaitGroup
		wg1.Add(num)
		wg2.Add(num)
		for v := 0; v < num; v++ {
			go func(v int) {
				ctx := context.Background()

				client, err := haraqa.NewClient(haraqa.DefaultConfig)
				if err != nil {
					b.Fatal(err)
				}
				topic := []byte("consumable" + strconv.Itoa(v))
				client.CreateTopic(ctx, topic)

				batchSize := 200
				msgs := make([][]byte, batchSize)
				for i := range msgs {
					msgs[i] = make([]byte, 100)
					rand.Read(msgs[i])
				}

				if cfg.Queue == nil {
					for i := 0; i < b.N/num; i += batchSize {
						err := client.Produce(ctx, topic, msgs...)
						if err != nil {
							b.Fatal(err)
						}
					}
				}

				var offset int64
				var resp haraqa.ConsumeResponse
				wg1.Done()
				<-ch
				for offset < int64(b.N/num) {
					err = client.Consume(ctx, topic, offset, int64(batchSize), &resp)
					if err != nil {
						b.Fatal(err)
					}

					// TODO: benchmark against Batch() function
					if next {
						for err == nil {
							discard, err = resp.Next()
							offset++
						}
						if err != io.EOF {
							b.Fatal(err)
						}
					} else {
						db, err := resp.Batch()
						if err != nil {
							b.Fatal(err)
						}
						discardBatch = db
						offset += int64(len(db))
					}

				}
				wg2.Done()

			}(v)
		}

		wg1.Wait()
		b.ReportAllocs()
		b.ResetTimer()
		close(ch)
		wg2.Wait()
		b.StopTimer()
	}
}

var discard []byte
var discardBatch [][]byte
