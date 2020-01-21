package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/haraqa/haraqa"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	topic := []byte("mytopic")
	// produce 100 messages to a topic
	producer, err := haraqa.NewClient(haraqa.DefaultConfig)
	check(err)
	msgs := make([][]byte, 100)
	for i := range msgs {
		msgs[i] = []byte(fmt.Sprintf("Message number '%v'", i))
	}
	err = producer.Produce(context.Background(), topic, msgs...)
	check(err)

	var totalCount int64
	var wg sync.WaitGroup
	group := []byte("my_group")
	// start 5 consumers in a common group, consuming max 5 messages at a time
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			client, err := haraqa.NewClient(haraqa.DefaultConfig)
			check(err)
			defer client.Close()

			for totalCount != 100 {
				count := newConsumer(client, topic, group)
				atomic.AddInt64(&totalCount, count)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func newConsumer(client haraqa.Client, topic []byte, groupName []byte) int64 {
	ctx := context.Background()

	// lock the consumer group, block until locked
	lock, _, err := client.Lock(ctx, append(groupName, topic...), true)
	check(err)

	// unlock to free next consumer
	defer lock.Close()

	// get the next available offset
	nextOffset := getNextOffset(client, groupName, topic)

	// consume starting at that next offset
	msgs, err := client.Consume(ctx, topic, nextOffset, 5, nil)
	check(err)

	if len(msgs) == 0 {
		return 0
	}

	// process the messages
	for _, msg := range msgs {
		doSomething(msg)
	}

	// set the next offset for another consumer
	setNextOffset(client, groupName, topic, nextOffset+int64(len(msgs)))

	return int64(len(msgs))
}

// getNextOffset gets the next offset for a topic/group
//  this uses haraqa, but could also be a cache or data store
func getNextOffset(client haraqa.Client, group, topic []byte) int64 {
	ctx := context.Background()

	// get last offset
	msgs, err := client.Consume(ctx, append(group, topic...), -1, 5, nil)

	// create and retry if topic does not exist
	if err == haraqa.ErrTopicDoesNotExist {
		err = client.CreateTopic(ctx, append(group, topic...))
		if err != haraqa.ErrTopicExists {
			check(err)
		}
		msgs, err = client.Consume(ctx, append(group, topic...), -1, 5, nil)
	}
	check(err)

	// read the bytes into an int64
	var nextOffset int64
	if len(msgs) != 0 && len(msgs[len(msgs)-1]) == 8 {
		nextOffset = int64(binary.BigEndian.Uint64(msgs[len(msgs)-1]))
	}
	return nextOffset
}

func setNextOffset(client haraqa.Client, group, topic []byte, offset int64) {
	msg := [8]byte{}
	binary.BigEndian.PutUint64(msg[:], uint64(offset))
	err := client.Produce(context.Background(), append(group, topic...), msg[:])
	check(err)
}

func doSomething(msg []byte) {
	fmt.Println(string(msg))
}
