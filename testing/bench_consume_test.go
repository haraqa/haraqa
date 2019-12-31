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
	"github.com/haraqa/haraqa/protocol"
	"github.com/pkg/errors"
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
			_, err := io.CopyN(ioutil.Discard, tcpConn, n)
			return err
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
			_, err := tcpConn.Write(writeBuf[:totalSize])
			return err
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
				err = client.CreateTopic(ctx, topic)
				if err != nil && errors.Cause(err).Error() != protocol.TopicExistsErr.Error() {
					errs <- err
					wg1.Done()
					return
				}

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

				if cfg.Queue == nil {
					for i := 0; i < b.N/num; i += batchSize {
						err := client.Produce(ctx, topic, msgs...)
						if err != nil {
							errs <- err
							wg1.Done()
							return
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
						errs <- err
						return
					}

					// TODO: benchmark against Batch() function
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
