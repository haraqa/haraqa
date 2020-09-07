package filequeue

import (
	"bytes"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/haraqa/haraqa/internal/headers"
	"github.com/pkg/errors"
)

func TestSortableDirNames(t *testing.T) {
	names := []string{
		formatName(1),
		formatName(0) + ".log",
		formatName(1) + ".log",
		formatName(0),
		formatName(2),
	}
	sort.Sort(sortableDirNames(names))
	n := int64(2)
	for _, name := range names {
		if strings.HasSuffix(name, ".log") {
			continue
		}
		if name != formatName(n) {
			t.Error(names, name, n)
		}
		n--
	}
}

func TestFileQueue_Produce(t *testing.T) {
	topic := "produce-topic"
	_ = os.RemoveAll(".haraqa-producer")
	defer os.RemoveAll(".haraqa-producer")

	q, err := New(".haraqa-producer")
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err := q.Close(); err != nil {
			t.Error(err)
		}
	}()

	// no messages
	err = q.Produce(topic, nil, 0, nil)
	if err != nil {
		t.Error(err)
	}

	// no body
	err = q.Produce(topic, []int64{123}, 0, nil)
	if !errors.Is(err, headers.ErrInvalidBodyMissing) {
		t.Error(err)
	}

	// no topic
	err = q.Produce(topic, []int64{123}, 0, bytes.NewBuffer(nil))
	if !errors.Is(err, headers.ErrTopicDoesNotExist) {
		t.Error(err)
	}

	err = q.CreateTopic(topic)
	if err != nil {
		t.Error(err)
	}
	input := []byte("some test input here")
	r := bytes.NewBuffer(nil)

	// produce once
	{
		if _, err = r.Write(input); err != nil {
			t.Error(err)
		}
		err = q.Produce(topic, []int64{5, int64(len(input) - 5)}, uint64(time.Now().Unix()), r)
		if err != nil {
			t.Error(err)
		}
	}

	// produce a second time off of cache
	{
		if _, err = r.Write(input); err != nil {
			t.Error(err)
		}
		err = q.Produce(topic, []int64{5, int64(len(input) - 5)}, uint64(time.Now().Unix()), r)
		if err != nil {
			t.Error(err)
		}
	}

	// produce && exceed max entries
	{
		q.max = 2
		if _, err = r.Write(input); err != nil {
			t.Error(err)
		}
		err = q.Produce(topic, []int64{5, int64(len(input) - 5)}, uint64(time.Now().Unix()), r)
		if err != nil {
			t.Error(err)
		}
	}

	// produce w/o cache
	{
		q.produceCache = nil
		if _, err = r.Write(input); err != nil {
			t.Error(err)
		}
		err = q.Produce(topic, []int64{5, int64(len(input) - 5)}, uint64(time.Now().Unix()), r)
		if err != nil {
			t.Error(err)
		}
	}

}
