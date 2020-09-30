package filequeue

import (
	"bytes"
	"io/ioutil"
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
	group := "consume-group"
	_ = os.RemoveAll(".haraqa-consumer")
	defer os.RemoveAll(".haraqa-consumer")
	q, err := New(true, 5000, ".haraqa-consumer")
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err := q.Close(); err != nil {
			t.Error(err)
		}
	}()

	// topic doesn't exist
	_, err = q.Consume(group, topic, 0, -1, nil)
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
		n, err := q.Consume(group, topic, 0, -1, w)
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

		b, err := ioutil.ReadAll(w.Body)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(b, bytes.Join(inputs, nil)) {
			t.Error(len(b), string(b))
		}
	}

	// consume again w/cache
	{
		w := httptest.NewRecorder()
		n, err := q.Consume(group, topic, 0, 2, w)
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
		b, err := ioutil.ReadAll(w.Body)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(b, bytes.Join(inputs[:2], nil)) {
			t.Error(len(b), string(b))
		}
	}

	// consume again w/offset
	{
		w := httptest.NewRecorder()
		n, err := q.Consume(group, topic, 2, -1, w)
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
		b, err := ioutil.ReadAll(w.Body)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(b, bytes.Join(inputs[2:], nil)) {
			t.Error(len(b), string(b))
		}
	}

	// consume just the last
	{
		w := httptest.NewRecorder()
		n, err := q.Consume(group, topic, -1, -1, w)
		if err != nil {
			t.Error(err)
		}
		if n != 1 {
			t.Error(n)
		}
		err = headers.ReadErrors(w.Header())
		if err != nil {
			t.Error(err)
		}
		sizes, err := headers.ReadSizes(w.Header())
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(sizes, []int64{msgSizes[len(msgSizes)-1]}) {
			t.Error(sizes, msgSizes)
		}
		b, err := ioutil.ReadAll(w.Body)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(b, inputs[len(inputs)-1]) {
			t.Error(len(b), string(b))
		}
	}
	// consume w/ multiple files
	{
		q.max = int64(len(inputs) - 1)
		newInput := []byte("additional file input")
		if _, err = r.Write(newInput); err != nil {
			t.Error(err)
		}
		err = q.Produce(topic, []int64{int64(len(newInput))}, 0, r)
		if err != nil {
			t.Error(err)
		}
		w := httptest.NewRecorder()
		n, err := q.Consume(group, topic, int64(len(inputs)), -1, w)
		if err != nil {
			t.Error(err)
		}
		if n != 1 {
			t.Error(n)
		}
		err = headers.ReadErrors(w.Header())
		if err != nil {
			t.Error(err)
		}
		sizes, err := headers.ReadSizes(w.Header())
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(sizes, []int64{int64(len(newInput))}) {
			t.Error(sizes, msgSizes)
		}
		b, err := ioutil.ReadAll(w.Body)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(b, newInput) {
			t.Error(len(b), string(b))
		}
	}
}
