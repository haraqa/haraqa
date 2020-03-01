package main

import (
	"context"
	"encoding/json"
	"runtime"
	"time"

	"github.com/haraqa/haraqa"
)

func main() {
	// connect to the broker
	client, err := haraqa.NewClient()
	if err != nil {
		panic(err)
	}
	ch := make(chan haraqa.ProduceMsg, 1024)

	// start producing metrics in the background
	go func() {
		t := time.NewTicker(time.Second)
		for range t.C {
			metrics := getMetrics()
			msg, err := json.Marshal(&metrics)
			if err != nil {
				panic(err)
			}
			ch <- haraqa.NewProduceMsg(msg)
		}
	}()

	// produce messages from ch
	err = client.ProduceLoop(context.Background(), []byte("memstats"), ch)
	if err != nil {
		panic(err)
	}
}

func getMetrics() interface{} {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return struct {
		Timestamp int64
		Memstats  runtime.MemStats
	}{
		Timestamp: time.Now().UnixNano(),
		Memstats:  m,
	}
}
