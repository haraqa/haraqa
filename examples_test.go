package haraqa_test

import (
	"context"
	"log"
	"time"

	"github.com/haraqa/haraqa"
)

// Example of the recommended way to create a new topic. An error is returned if
// that topic already exists
func Example_createTopic() {
	ctx := context.Background()
	client, err := haraqa.NewClient()
	if err != nil {
		panic(err)
	}

	topic := []byte("myTopic")
	err = client.CreateTopic(ctx, topic)
	if err != nil && err != haraqa.ErrTopicExists {
		panic(err)
	}
}

func Example_deleteTopic() {
	ctx := context.Background()
	client, err := haraqa.NewClient()
	if err != nil {
		panic(err)
	}

	topic := []byte("myTopic")
	err = client.DeleteTopic(ctx, topic)
	if err != nil {
		panic(err)
	}
}

func Example_listTopics() {
	ctx := context.Background()
	client, err := haraqa.NewClient()
	if err != nil {
		panic(err)
	}

	// set prefix, suffix and/or regex to match topics with
	// if all are blank, retrieve all topics
	prefix, suffix, regex := "", "", ""

	// list topics
	topics, err := client.ListTopics(ctx, prefix, suffix, regex)
	if err != nil {
		panic(err)
	}

	for _, topic := range topics {
		log.Println("Found topic:", string(topic))
	}
}

func Example_watcher() {
	ctx := context.Background()
	client, err := haraqa.NewClient()
	if err != nil {
		panic(err)
	}

	// get all topics
	prefix, suffix, regex := "", "", ""
	topics, err := client.ListTopics(ctx, prefix, suffix, regex)
	if err != nil {
		panic(err)
	}

	// only watch for 20 seconds (optional)
	ctx, cancel := context.WithTimeout(ctx, time.Second*20)
	defer cancel()

	// start watching
	w, err := client.NewWatcher(ctx, topics...)
	if err != nil {
		panic(err)
	}

	// Listen for new offsets
	for event := range w.Events() {
		log.Printf("new messages are ready for topic %q, min offset: %d, max offset: %d\n", string(event.Topic), event.MinOffset, event.MaxOffset)
	}

	// Close and check for errors
	err = w.Close()
	if err != nil && err != ctx.Err() {
		panic(err)
	}
}

func Example_offsets() {
	ctx := context.Background()
	client, err := haraqa.NewClient()
	if err != nil {
		panic(err)
	}

	// get the minimum and maximum available offsets of a topic
	topic := []byte("myTopic")
	min, max, err := client.Offsets(ctx, topic)
	if err != nil {
		panic(err)
	}

	log.Println("min:", min, "max:", max)
}

func Example_produce() {
	client, err := haraqa.NewClient()
	if err != nil {
		panic(err)
	}

	var (
		ctx   = context.Background()
		topic = []byte("myTopic")
		msg   = []byte("my message")
	)

	err = client.Produce(ctx, topic, msg)
	if err != nil {
		panic(err)
	}
}

//Example of the recommended way to produce messages. A ProduceLoop function
// runs in the background and new messages are sent via a channel to be produced.
// Messages are batched to increase efficiency
func Example_produceLoop() {
	client, err := haraqa.NewClient()
	if err != nil {
		panic(err)
	}

	var (
		ctx   = context.Background()
		topic = []byte("myTopic")
		msg1  = haraqa.NewProduceMsg([]byte("my message"))
		msg2  = haraqa.NewProduceMsg([]byte("my other message"))

		// the capacity of the channel determines the maximum allowed batch size
		ch = make(chan haraqa.ProduceMsg, 1000)
	)

	// start the loop in the background
	go func() {
		err = client.ProduceLoop(ctx, topic, ch)
		if err != nil {
			panic(err)
		}
	}()

	//closing the channel will gracefully close ProduceLoop
	defer close(ch)

	//sending messages to the channel is thread safe and can be done from multiple goroutines
	ch <- msg1
	ch <- msg2

	//errors are returned to each produce message
	err = <-msg1.Err
	if err != nil {
		panic(err)
	}
	err = <-msg2.Err
	if err != nil {
		panic(err)
	}
}

// Example for consuming messages all at once
func Example_consume() {
	client, err := haraqa.NewClient()
	if err != nil {
		panic(err)
	}

	var (
		ctx   = context.Background()
		topic = []byte("myTopic")

		offset int64 = 0    // start at oldest available message
		limit  int64 = 1000 // maximum number of messages to return
	)
	msgs, err := client.Consume(ctx, topic, offset, limit, nil)
	if err != nil {
		panic(err)
	}

	for i := range msgs {
		log.Println(msgs[i])
	}
}

func Example_consumeBuffer() {
	// when consuming in a loop, it can be more efficient to
	// use a buffer to avoid unnecessary allocations.
	// Messages should be processed or copied prior to reusing the buffer

	client, err := haraqa.NewClient()
	if err != nil {
		panic(err)
	}

	var (
		ctx   = context.Background()
		topic = []byte("myTopic")

		offset int64 = 0    // start at oldest available message
		limit  int64 = 1000 // maximum number of messages to return
	)

	buf := haraqa.NewConsumeBuffer()
	for {
		msgs, err := client.Consume(ctx, topic, offset, limit, buf)
		if err != nil {
			panic(err)
		}

		if len(msgs) == 0 {
			break
		}

		for i := range msgs {
			log.Println(msgs[i])
			offset++
		}
	}
}

func Example_lock() {
	client, err := haraqa.NewClient()
	if err != nil {
		panic(err)
	}
	topic := "mytopic"
	group := "mygroup"

	ctx := context.Background()

	// make a lock for this group on this topic
	blocking := false
	lock, locked, err := client.Lock(ctx, []byte(group+topic), blocking)
	if err != nil {
		panic(err)
	}
	if !locked {
		log.Println("could not get lock")
		return
	}
	defer lock.Close()

	// we have the lock, now do something

	// ...
}
