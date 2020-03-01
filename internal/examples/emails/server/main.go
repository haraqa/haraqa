package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/haraqa/haraqa"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	go senderLoop()

	client, err := haraqa.NewClient()
	check(err)

	http.Handle("/profile/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username := strings.Split(strings.TrimPrefix(r.URL.Path, "/profile/"), "/")[0]
		addView(client, username)
		w.Write([]byte("Congrats, you're looking at the profile of " + username))
	}))
	http.ListenAndServe(":8080", nil)
	client.Close()
}

func addView(client *haraqa.Client, username string) {
	ctx := context.Background()
	err := client.Produce(ctx, []byte("profile-views:"+username), []byte{0})
	check(err)
}

func senderLoop() {
	client, err := haraqa.NewClient()
	check(err)
	defer client.Close()

	ctx := context.Background()
	t := time.NewTicker(time.Hour * 24)
	buf := haraqa.NewConsumeBuffer()
	for range t.C {
		topics, err := client.ListTopics(ctx, "profile-views:", "", "")
		check(err)

		for _, topic := range topics {
			split := bytes.SplitN(topic, []byte{':'}, 2)
			if len(split) < 2 {
				continue
			}
			username := string(split[1])
			startingOffset := getTopicOffset(ctx, client, topic)
			offset := startingOffset
			for {
				msgs, err := client.Consume(ctx, topic, offset, 1024, buf)
				if err == haraqa.ErrTopicDoesNotExist {
					break
				}
				check(err)
				if len(msgs) == 0 {
					break
				}
				offset += int64(len(msgs))
			}
			count := offset - startingOffset
			if count == 0 {
				continue
			}

			sendEmail(username, count)
			setTopicOffset(ctx, client, topic, offset)
		}
	}
}

func getTopicOffset(ctx context.Context, client *haraqa.Client, topic []byte) int64 {
	// this would be where you call a database, cache or another topic
	// we'll use haraqa as our datastore
	offsetTopic := []byte("emailSender:" + string(topic))

	msgs, err := client.Consume(ctx, offsetTopic, -1, 1024, nil)
	if err == haraqa.ErrTopicDoesNotExist {
		return 0
	}
	check(err)
	if len(msgs) == 0 {
		return 0
	}

	var msg [8]byte
	copy(msg[:], msgs[len(msgs)-1])
	offset := int64(binary.BigEndian.Uint64(msg[:]))

	return offset
}

func setTopicOffset(ctx context.Context, client *haraqa.Client, topic []byte, offset int64) {
	offsetTopic := []byte("emailSender:" + string(topic))

	var msg [8]byte
	binary.BigEndian.PutUint64(msg[:], uint64(offset))
	err := client.Produce(ctx, offsetTopic, msg[:])
	check(err)
}

func sendEmail(username string, count int64) {
	log.Printf("Sending an email to %s that there have been %d profile views\n", username, count)
	// send email logic not included
}
