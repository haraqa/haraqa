package main

import (
	"context"
	"crypto/rand"
	"os"
	"time"

	"github.com/haraqa/haraqa"
	"github.com/haraqa/haraqa/internal/examples/encrypted"
)

func main() {
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

	t := time.NewTicker(time.Hour * 6)
	for range t.C {
		newKey := make([]byte, 96)
		_, err = rand.Read(newKey)
		if err != nil {
			panic(err)
		}
		err = encClient.Produce(ctx, topic, newKey)
		if err != nil {
			panic(err)
		}
	}
}
