package filequeue

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/haraqa/haraqa/internal/headers"
)

func TestFileQueue_ModifyTopic(t *testing.T) {
	dir := ".haraqa-modify"
	topic := "modify-topic"

	_ = os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	q, err := New(false, 2, dir)
	if err != nil {
		t.Fatal(err)
	}
	err = q.CreateTopic(topic)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.Produce(topic, []int64{5, 5}, uint64(time.Now().Unix()), bytes.NewBuffer([]byte("helloworld"))); err != nil {
		t.Error(err)
	}
	if err = q.Produce(topic, []int64{5, 5}, uint64(time.Now().Unix()), bytes.NewBuffer([]byte("hellothere"))); err != nil {
		t.Error(err)
	}
	if err = q.Produce(topic, []int64{5, 5}, uint64(time.Now().Unix()), bytes.NewBuffer([]byte("helloagain"))); err != nil {
		t.Error(err)
	}
	if tmp, err := os.Create(filepath.Join(dir, topic, "invalid-file")); err != nil {
		t.Error(err)
	} else {
		_ = tmp.Close()
	}

	// empty request
	info, err := q.ModifyTopic("", headers.ModifyRequest{})
	if err != nil || info != nil {
		t.Error(err, info)
	}

	info, err = q.ModifyTopic(topic, headers.ModifyRequest{
		Truncate: 3,
	})
	if err != nil {
		t.Error(err)
	}
	if info == nil || info.MinOffset != 2 || info.MaxOffset != 5 {
		t.Error(info)
	}

	info, err = q.ModifyTopic(topic, headers.ModifyRequest{
		Truncate: -1,
	})
	if err != nil {
		t.Error(err)
	}
	if info == nil || info.MinOffset != 4 || info.MaxOffset != 5 {
		t.Error(info)
	}

	info, err = q.ModifyTopic(topic, headers.ModifyRequest{
		Before: time.Now(),
	})
	if err != nil {
		t.Error(err)
	}
	if info == nil || info.MinOffset != 0 || info.MaxOffset != 0 {
		t.Error(info)
	}
}
