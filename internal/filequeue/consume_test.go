package filequeue

import (
	"bytes"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/haraqa/haraqa/internal/headers"
	"github.com/pkg/errors"
)

func TestFileQueue_Consume(t *testing.T) {
	topic := "consume-topic"
	_ = os.RemoveAll(".haraqa-consumer")
	defer os.RemoveAll(".haraqa-consumer")
	q, err := New(".haraqa-consumer")
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err := q.Close(); err != nil {
			t.Error(err)
		}
	}()

	// topic doesn't exist
	_, err = q.Consume(topic, 0, -1, nil)
	if !errors.Is(err, headers.ErrTopicDoesNotExist) {
		t.Error(err)
	}

	inputs := [][]byte{
		[]byte("hello world"),
		[]byte("hello there"),
		[]byte("This is a test msg"),
		[]byte("this is another"),
		{0, 1, 2, 3, 254, 255},
	}
	r := bytes.NewBuffer(nil)
	var msgSizes []int64
	for _, input := range inputs {
		msgSizes = append(msgSizes, int64(len(input)))
		_, err = r.Write(input)
		if err != nil {
			t.Error(err)
		}
	}
	if err = q.CreateTopic(topic); err != nil {
		t.Error(err)
	}
	if err = q.Produce(topic, msgSizes, uint64(time.Now().Unix()), r); err != nil {
		t.Error(err)
	}
	// consume
	{
		w := httptest.NewRecorder()
		n, err := q.Consume(topic, 0, -1, w)
		if err != nil {
			t.Error(err)
		}
		if n != len(inputs) {
			t.Error(n, len(inputs))
		}
		err = headers.ReadErrors(w.Header())
		if err != nil {
			t.Error(err)
		}
		sizes, err := headers.ReadSizes(w.Header())
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(sizes, msgSizes) {
			t.Error(sizes, msgSizes)
		}
	}

	// consume again w/cache
	{
		w := httptest.NewRecorder()
		n, err := q.Consume(topic, 0, 2, w)
		if err != nil {
			t.Error(err)
		}
		if n != 2 {
			t.Error(n, len(inputs))
		}
		err = headers.ReadErrors(w.Header())
		if err != nil {
			t.Error(err)
		}
		sizes, err := headers.ReadSizes(w.Header())
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(sizes, msgSizes[:2]) {
			t.Error(sizes, msgSizes)
		}
	}

	// consume again w/offset
	{
		w := httptest.NewRecorder()
		n, err := q.Consume(topic, 2, -1, w)
		if err != nil {
			t.Error(err)
		}
		if n != len(inputs)-2 {
			t.Error(n, len(inputs))
		}
		err = headers.ReadErrors(w.Header())
		if err != nil {
			t.Error(err)
		}
		sizes, err := headers.ReadSizes(w.Header())
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(sizes, msgSizes[2:]) {
			t.Error(sizes, msgSizes)
		}
	}
}
