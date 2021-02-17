package queue

import (
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/haraqa/haraqa/internal/headers"

	"github.com/pkg/errors"
)

func TestQueue_CreateTopic(t *testing.T) {
	dirName, err := os.MkdirTemp("", ".haraqa*")
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err := os.RemoveAll(dirName); err != nil {
			t.Error(err)
		}
	}()

	q := &Queue{
		dirs: []string{dirName},
	}
	if err = q.CreateTopic("new_topic"); err != nil {
		t.Error(err)
	}
	if err = q.CreateTopic("new_topic"); err != nil {
		t.Error(err)
	}
	if err = q.CreateTopic("../new_topic"); err.Error() != "invalid topic" {
		t.Error(err)
	}
	if err = q.CreateTopic("other/thing"); !errors.Is(err, os.ErrNotExist) {
		t.Error(err)
	}
}

func TestQueue_ListTopics(t *testing.T) {
	dirName, err := os.MkdirTemp("", ".haraqa*")
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err := os.RemoveAll(dirName); err != nil {
			t.Error(err)
		}
	}()

	q := &Queue{
		dirs: []string{dirName},
	}
	if err = q.CreateTopic("topic_a"); err != nil {
		t.Error(err)
	}
	if err = q.CreateTopic("topic_b"); err != nil {
		t.Error(err)
	}
	if err = q.CreateTopic("t_a"); err != nil {
		t.Error(err)
	}

	topics, err := q.ListTopics("topic", "", "")
	if err != nil {
		t.Error(err)
	}
	sort.Strings(topics)
	if len(topics) != 2 || topics[0] != "topic_a" || topics[1] != "topic_b" {
		t.Error(topics)
	}

	topics, err = q.ListTopics("", "a", "")
	if err != nil {
		t.Error(err)
	}
	sort.Strings(topics)
	if len(topics) != 2 || topics[0] != "t_a" || topics[1] != "topic_a" {
		t.Error(topics)
	}

	topics, err = q.ListTopics("", "", "topic.*")
	if err != nil {
		t.Error(err)
	}
	sort.Strings(topics)
	if len(topics) != 2 || topics[0] != "topic_a" || topics[1] != "topic_b" {
		t.Error(topics)
	}

	_, err = q.ListTopics("", "", "*")
	if !strings.Contains(err.Error(), "error parsing regexp") {
		t.Error(err)
	}

	q.dirs[0] = "folder/doesnt/exist"
	_, err = q.ListTopics("", "", "")
	if !errors.Is(err, os.ErrNotExist) {
		t.Error(err)
	}
}

func TestQueue_GetTopicOwner(t *testing.T) {
	owner, err := (&Queue{}).GetTopicOwner("topic")
	if err != nil {
		t.Error(err)
	}
	if owner != "" {
		t.Error(owner)
	}
}

func TestQueue_DeleteTopic(t *testing.T) {
	dirName, err := os.MkdirTemp("", ".haraqa*")
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err := os.RemoveAll(dirName); err != nil {
			t.Error(err)
		}
	}()

	q := &Queue{
		dirs: []string{dirName},
	}
	if err = q.CreateTopic("topic"); err != nil {
		t.Error(err)
	}
	if err = q.DeleteTopic("topic"); err != nil {
		t.Error(err)
	}
	if err = q.DeleteTopic("topic"); err != nil {
		t.Error(err)
	}

	if err = q.DeleteTopic("../new_topic"); err.Error() != "invalid topic" {
		t.Error(err)
	}
}

func testModifyTopic(q *Queue, topic string, req headers.ModifyRequest, min, max int64) func(*testing.T) {
	return func(t *testing.T) {
		info, err := q.ModifyTopic(topic, req)
		if err != nil {
			t.Error(err)
		}
		if info.MinOffset != min || info.MaxOffset != max {
			d, err := os.Open(filepath.Join(q.RootDir(), topic))
			if err == nil {
				defer d.Close()
				t.Log(d.Readdirnames(-1))
			}
			t.Error(info)
		}
	}
}

func TestQueue_ModifyTopic(t *testing.T) {
	dirName, err := os.MkdirTemp("", ".haraqa*")
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err := os.RemoveAll(dirName); err != nil {
			t.Error(err)
		}
	}()
	q, err := NewQueue([]string{dirName}, false, 2)
	if err != nil {
		t.Error(err)
	}
	const topic = "topic"
	if err = q.CreateTopic(topic); err != nil {
		t.Error(err)
	}
	info, err := q.ModifyTopic(topic, headers.ModifyRequest{})
	if err != nil {
		t.Error(err)
	}
	if info.MinOffset != 0 || info.MaxOffset != 0 {
		t.Error(info)
	}

	r := new(bytes.Buffer)
	var msgSizes []int64
	for _, msg := range []string{"my", "test", "messages", "are", "here"} {
		msgSizes = append(msgSizes, int64(len(msg)))
		r.WriteString(msg)
	}
	if err := q.Produce(topic, msgSizes, uint64(time.Now().Unix()), r); err != nil {
		t.Error(err)
	}

	t.Run("trunc 2", testModifyTopic(q, topic, headers.ModifyRequest{Truncate: 2}, 2, 5))
	t.Run("trunc 3", testModifyTopic(q, topic, headers.ModifyRequest{Truncate: 3}, 2, 5))
	t.Run("trunc 4", testModifyTopic(q, topic, headers.ModifyRequest{Truncate: 4}, 4, 5))
	t.Run("trunc 5", testModifyTopic(q, topic, headers.ModifyRequest{Truncate: 4}, 4, 5))
	t.Run("trunc 6", testModifyTopic(q, topic, headers.ModifyRequest{Truncate: 4}, 4, 5))
	t.Run("before now", testModifyTopic(q, topic, headers.ModifyRequest{Before: time.Now()}, 4, 5))
}
