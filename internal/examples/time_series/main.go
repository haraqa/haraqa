package main

import (
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
	defer client.Close()

	producer, err := client.NewProducer(haraqa.WithTopic([]byte("memstats")), haraqa.WithIgnoreErrors())
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	// start producing metrics in the background
	t := time.NewTicker(time.Second)
	for range t.C {
		metrics := getMetrics()
		msg, err := json.Marshal(&metrics)
		if err != nil {
			panic(err)
		}
		producer.Send(msg)
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
