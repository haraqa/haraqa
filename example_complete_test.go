package haraqa_test

import (
	"context"
	"log"

	"github.com/haraqa/haraqa"
)

func Example() {
	ctx := context.Background()
	topic := []byte("my_topic")
	maxBatchSize := int64(2048)

	// setup a channel to send batches of messages
	ch := make(chan haraqa.ProduceMsg, maxBatchSize)

	// make new client & connect to broker
	config := haraqa.DefaultConfig
	client, _ := haraqa.NewClient(config)
	defer client.Close()

	// create a new topic
	_ = client.CreateTopic(ctx, topic)

	// list all topics matching the given prefix+suffix+regex
	prefix, suffix, regex := "", "", ""
	topics, _ := client.ListTopics(ctx, prefix, suffix, regex)
	for i := range topics {
		log.Println("found topic", string(topics[i]))
	}

	// start a producer loop in the background
	go func() {
		_ = client.ProduceLoop(ctx, topic, ch)
	}()

	// send 10 messages
	for i := 0; i < 10; i++ {
		go sendMessage(ch)
	}

	// get the minimum and maximum available offsets
	minOffset, maxOffset, _ := client.Offsets(ctx, topic)
	log.Println(minOffset, maxOffset)

	// start a watcher on the topic, this will notify of new topic offsets
	watchEvents := make(chan haraqa.WatchEvent, 1)
	watchCloser, _ := client.WatchTopics(ctx, watchEvents, topic)
	defer watchCloser.Close()

	// start consuming from the oldest message in the queue
	offset := minOffset

	// for our example, we want to stop after consuming 10 messages
	for offset < 10 {
		// start consuming starting at the offset
		msgs, _ := client.Consume(ctx, topic, offset, maxBatchSize, nil)
		for _, msg := range msgs {
			log.Println("Retrieved message:", string(msg))
		}

		// if no messages are returned listen to the watcher to know when more are available
		if len(msgs) == 0 {
			for {
				watchEvent := <-watchEvents
				if watchEvent.Err != nil {
					// handle error
				}
				if watchEvent.MaxOffset > maxOffset {
					maxOffset = watchEvent.MaxOffset
					break
				}
			}
			continue
		}

		// adjust the offset to consume the next batch
		offset += int64(len(msgs))
	}

	// re-consume a message, this time targeting the newest message in the queue
	msgs, _ := client.Consume(ctx, topic, -1, 2048, nil)
	log.Println("Re-retrieved message:", string(msgs[len(msgs)-1]))

	// stop the background producer loop
	close(ch)

	// delete the topic
	_ = client.DeleteTopic(ctx, topic)
}

func sendMessage(ch chan haraqa.ProduceMsg) error {
	msg := haraqa.NewProduceMsg([]byte("hello world"))
	ch <- msg
	err := <-msg.Err
	return err
}
