package queue

import (
	"bytes"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/haraqa/haraqa/internal/headers"
)

func TestQueue_Consume(t *testing.T) {
	dirName, err := os.MkdirTemp("", ".haraqa*")
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err := os.RemoveAll(dirName); err != nil {
			t.Error(err)
		}
	}()
	const maxEntries1 = 4
	const maxEntries2 = 2
	q, err := NewQueue([]string{dirName}, false, maxEntries1)
	if err != nil {
		t.Error(err)
	}
	const topic = "topic"
	if err = q.CreateTopic(topic); err != nil {
		t.Error(err)
	}

	r := new(bytes.Buffer)
	var msgSizes []int64
	msgs := []string{"my", "test", "messages", "are", "here"}
	for _, msg := range msgs[:maxEntries1] {
		msgSizes = append(msgSizes, int64(len(msg)))
		r.WriteString(msg)
	}
	if err := q.Produce(topic, msgSizes[:maxEntries1], uint64(time.Now().Unix()), r); err != nil {
		t.Error(err)
	}
	q.maxEntries = maxEntries2
	for _, msg := range msgs[maxEntries1:] {
		msgSizes = append(msgSizes, int64(len(msg)))
		r.WriteString(msg)
	}
	if err := q.Produce(topic, msgSizes[maxEntries1:], uint64(time.Now().Unix()), r); err != nil {
		t.Error(err)
	}

testSuite:
	t.Run("consume 0:-1", testConsume(q, "", topic, 0, -1, msgs[0:maxEntries1]))
	t.Run("consume 0:100", testConsume(q, "", topic, 0, 100, msgs[0:maxEntries1]))

	t.Run("consume 1:-1", testConsume(q, "", topic, 1, -1, msgs[1:maxEntries1]))
	t.Run("consume 1:100", testConsume(q, "", topic, 1, 100, msgs[1:maxEntries1]))
	t.Run("consume 1:3", testConsume(q, "", topic, 1, 2, msgs[1:3]))

	if err = q.ClearCache(); err != nil {
		t.Error(err)
	}

	t.Run("consume 3:-1", testConsume(q, "", topic, 3, -1, msgs[3:maxEntries1]))
	t.Run("consume 3:100", testConsume(q, "", topic, 3, 100, msgs[3:maxEntries1]))

	t.Run("consume 5:-1", testConsume(q, "", topic, 4, -1, msgs[4:]))
	t.Run("consume 5:100", testConsume(q, "", topic, 4, 100, msgs[4:]))

	if err = q.Close(); err != nil {
		t.Error(err)
	}
	if q.fileCache != nil {
		return
	}
	q, err = NewQueue([]string{dirName}, true, maxEntries2)
	if err != nil {
		t.Error(err)
	}
	goto testSuite
}

func testConsume(q *Queue, group, topic string, id, limit int64, expected []string) func(*testing.T) {
	return func(t *testing.T) {
		w := httptest.NewRecorder()
		n, err := q.Consume(group, topic, id, limit, w)
		if err != nil {
			t.Error(err)
		}
		if n != len(expected) {
			t.Error(n)
		}
		resp := w.Result()
		if err = headers.ReadErrors(resp.Header); err != nil {
			t.Error(err)
		}
		sizes, err := headers.ReadSizes(resp.Header)
		if err != nil {
			t.Error(err)
		}
		if len(sizes) != len(expected) {
			t.Error(sizes)
			d, err := os.Open(filepath.Join(q.RootDir(), topic))
			if err == nil {
				defer d.Close()
				t.Log(d.Readdirnames(-1))
			}
		}
		for i := range sizes {
			if int(sizes[i]) != len(expected[i]) {
				t.Error(i, sizes[i], expected[i])
			}
		}
	}
}
