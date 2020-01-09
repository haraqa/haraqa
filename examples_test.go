package haraqa_test

import (
	"context"
	"log"

	"github.com/haraqa/haraqa"
)

// Example of the recommended way to create a new topic. An error is returned if
// that topic already exists
func ExampleClient_CreateTopic() {
	ctx := context.Background()
	config := haraqa.DefaultConfig
	client, err := haraqa.NewClient(config)
	if err != nil {
		panic(err)
	}

	topic := []byte("myTopic")
	err = client.CreateTopic(ctx, topic)
	if err != nil && err != haraqa.ErrTopicExists {
		panic(err)
	}
}

func ExampleClient_Produce() {
	config := haraqa.DefaultConfig
	client, err := haraqa.NewClient(config)
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
func ExampleClient_ProduceLoop() {
	config := haraqa.DefaultConfig
	client, err := haraqa.NewClient(config)
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
func ExampleClient_Consume_batch() {
	config := haraqa.DefaultConfig
	client, err := haraqa.NewClient(config)
	if err != nil {
		panic(err)
	}

	var (
		ctx   = context.Background()
		topic = []byte("myTopic")

		offset       int64 = 0    // start at oldest available message
		maxBatchSize int64 = 1000 // maximum number of messages to return
	)
	msgs, err := client.Consume(ctx, topic, offset, maxBatchSize, nil)
	if err != nil {
		panic(err)
	}

	for i := range msgs {
		log.Println(msgs[i])
	}
}
