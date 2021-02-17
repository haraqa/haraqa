package queue

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/haraqa/haraqa/internal/headers"
)

func TestQueue_Produce(t *testing.T) {
	t.Run("no cache", testQueueProduce(false))
	t.Run("cached", testQueueProduce(true))
}

func testQueueProduce(cache bool) func(*testing.T) {
	return func(t *testing.T) {
		var err error
		dirNames := make([]string, 3)
		for i := range dirNames {
			dirNames[i], err = os.MkdirTemp("", ".haraqa*")
			if err != nil {
				t.Error(err)
			}
		}
		defer func() {
			for _, dirName := range dirNames {
				if err := os.RemoveAll(dirName); err != nil {
					t.Error(err)
				}
			}
		}()

		q, err := NewQueue(dirNames, cache, 2)
		if err != nil {
			t.Error(err)
		}
		defer q.Close()
		const topic = "topic"
		if err = q.CreateTopic(topic); err != nil {
			t.Error(err)
		}

		if err = q.Produce(topic, nil, 0, nil); err != nil {
			t.Error(err)
		}

		msgs := []string{"my", "test", "messages", "are", "here"}
		if err = produceMsgs(q, topic, msgs); err != nil {
			t.Error(err)
		}
		if err = produceMsgs(q, topic, msgs); err != nil {
			t.Error(err)
		}

		info, err := q.ModifyTopic(topic, headers.ModifyRequest{})
		if err != nil {
			t.Error(err)
		}
		if info.MinOffset != 0 || info.MaxOffset != int64(len(msgs)*2) {
			t.Error(info)
		}
	}
}

func produceMsgs(q *Queue, topic string, msgs []string) error {
	r := new(bytes.Buffer)
	var msgSizes []int64
	for _, msg := range msgs {
		msgSizes = append(msgSizes, int64(len(msg)))
		r.WriteString(msg)
	}
	return q.Produce(topic, msgSizes, uint64(time.Now().Unix()), r)
}
