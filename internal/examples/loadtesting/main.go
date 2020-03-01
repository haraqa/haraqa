package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/haraqa/haraqa"
	"github.com/pkg/errors"
)

func getInt(name string, defaultValue int) int {
	n := os.Getenv(name)
	if n == "" {
		return defaultValue
	}
	v, _ := strconv.Atoi(n)
	return v
}

func main() {
	wait := make(chan struct{})

	addr := os.Getenv("HOST")
	if addr == "" {
		addr = "127.0.0.1"
	}
	topic := os.Getenv("TOPIC")
	n := getInt("N", 250000)
	msgSize := getInt("MSG_SIZE", 100)
	batchSize := getInt("BATCH_SIZE", 2048)
	numTopics := getInt("NUM_TOPICS", 4)
	var wg sync.WaitGroup

	client, err := haraqa.NewClient(haraqa.WithAddr(addr))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	for i := 0; i < numTopics; i++ {
		cfg := produceConfig{
			addr:      addr,
			topic:     []byte(topic + "_" + strconv.Itoa(i)),
			n:         n,
			msgSize:   msgSize,
			batchSize: batchSize,
		}
		client.CreateTopic(context.Background(), cfg.topic)
		if err != nil && errors.Cause(err) != haraqa.ErrTopicExists {
			panic(err)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			err := produce(cfg, wait)
			if err != nil {
				panic(err)
			}
		}()
	}

	// wait for clock to get to the nearest 2 min (allows some rudimentary coordination across containers)
	now := time.Now()
	then := now.Add(time.Minute * 1).Round(time.Minute * 2)
	log.Println("Ready to go, starting at", then.String())
	<-time.After(then.Sub(time.Now()))

	// start the load, print the time
	start := time.Now()
	close(wait)
	wg.Wait()
	duration := time.Now().Sub(start)
	fmt.Printf("number of messages: %d, duration: %s\n", numTopics*n, duration.String())
	fmt.Printf("messages/sec: %f\n", float64(numTopics*n)/duration.Seconds())
}

type produceConfig struct {
	addr      string
	topic     []byte
	batchSize int
	n         int
	msgSize   int
}

func produce(config produceConfig, wait chan struct{}) error {
	client, err := haraqa.NewClient(haraqa.WithAddr(config.addr))
	if err != nil {
		return err
	}
	defer client.Close()
	ctx := context.Background()
	ch := make(chan haraqa.ProduceMsg, config.batchSize)

	go func() {
		msg := make([]byte, config.msgSize)
		io.ReadFull(rand.Reader, msg)
		<-wait
		defer close(ch)
		for j := 0; j < config.n; j++ {
			ch <- haraqa.ProduceMsg{Msg: msg}
		}
	}()

	err = client.ProduceLoop(ctx, config.topic, ch)
	if err != nil {
		return err
	}
	return nil
}
