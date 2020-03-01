package main

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/gorilla/sessions"
	"github.com/haraqa/haraqa"
	"github.com/haraqa/haraqa/internal/examples/encrypted"
)

func main() {

}

var storeLock sync.RWMutex
var store *sessions.CookieStore

func updateKey() {
	config := haraqa.DefaultConfig
	client, err := haraqa.NewClient(config)
	if err != nil {
		panic(err)
	}

	var aesKey [32]byte
	copy(aesKey[:], []byte(os.Getenv("SECRET_AESKEY")))
	encClient, err := encrypted.NewClient(client, aesKey)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	topic := []byte("aes-keys")

	var first [96]byte
	for first == [96]byte{} {
		msgs, err := encClient.Consume(ctx, topic, -1, 1, nil)
		if err != nil {
			panic(err)
		}
		if len(msgs) != 1 {
			time.Sleep(time.Second * 1)
			continue
		}
		copy(first[:], msgs[0])
	}
	firstAuth := make([]byte, 96)
	copy(firstAuth[:], first[:])

	storeLock.Lock()
	store = sessions.NewCookieStore(firstAuth[:64], firstAuth[64:])
	storeLock.Unlock()

	buf := haraqa.NewConsumeBuffer()
	t := time.NewTicker(time.Minute * 30)
	var second [96]byte
	for range t.C {
		msgs, err := encClient.Consume(ctx, topic, -1, 1, buf)
		if err != nil {
			panic(err)
		}
		if len(msgs) != 1 {
			continue
		}
		copy(second[:], msgs[0])
		if first == second {
			continue
		}

		newAuth := make([]byte, 96)
		oldAuth := make([]byte, 96)
		copy(newAuth[:], second[:])
		copy(oldAuth[:], first[:])

		storeLock.Lock()
		store = sessions.NewCookieStore(newAuth[:64], newAuth[64:], oldAuth[:64], oldAuth[64:])
		storeLock.Unlock()
		copy(first[:], second[:])
	}
}
