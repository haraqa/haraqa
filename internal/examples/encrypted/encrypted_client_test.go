package encrypted

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/haraqa/haraqa"
	"github.com/haraqa/haraqa/mock"
)

func TestEncryption(t *testing.T) {
	var onDisk [][]byte
	mockClient := mock.NewMockClient(gomock.NewController(t))
	mockClient.EXPECT().Produce(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, topic []byte, msgs ...[]byte) error {
			onDisk = msgs
			return nil
		}).Times(1)

	var client haraqa.Client
	var err error

	var key [32]byte
	copy(key[:], []byte("my secret key"))
	client, err = NewClient(mockClient, key)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	topic := []byte("my topic")
	err = client.Produce(ctx, topic, []byte("message 1"), []byte("message 2"))
	if err != nil {
		t.Fatal(err)
	}

	mockClient.EXPECT().Consume(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(onDisk, nil).Times(1)

	msgs, err := client.Consume(ctx, topic, 0, 2, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(msgs) != 2 || string(msgs[0]) != "message 1" || string(msgs[1]) != "message 2" {
		t.Fatal(fmt.Sprintf("%d %d %q %q", len(msgs[0]), len(msgs[1]), string(msgs[0]), string(msgs[1])))
	}
}
