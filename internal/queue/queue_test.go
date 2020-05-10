package queue

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/haraqa/haraqa/internal/protocol"
	"github.com/pkg/errors"
)

func TestNewQueue(t *testing.T) {
	_, err := NewQueue(nil, 0, 0)
	if err.Error() != "missing volumes from NewQueue call" {
		t.Fatal(err)
	}
	_, err = NewQueue([]string{".haraqa", ".haraqa"}, 0, 0)
	if errors.Cause(err).Error() != "found duplicate file .haraqa" {
		t.Fatal(err)
	}
	_, err = NewQueue([]string{string([]byte{0, 0, 0})}, 0, 0)
	if !strings.HasSuffix(err.Error(), "invalid argument") {
		t.Fatal(err)
	}
	_, err = NewQueue([]string{".haraqa-a", ".haraqa-b"}, 0, 0)
	defer func() {
		_ = os.RemoveAll(".haraqa-a")
		_ = os.RemoveAll(".haraqa-b")
	}()
	if err != nil {
		t.Fatal(err)
	}
	err = os.Mkdir(".haraqa-b/topic", os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(".haraqa-b/topic/0.dat")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	_, err = NewQueue([]string{".haraqa-c", ".haraqa-b"}, 0, 0)
	defer func() { _ = os.RemoveAll(".haraqa-c") }()
	if err != nil {
		t.Fatal(err)
	}
}

func TestQueueFiles(t *testing.T) {
	volumes := []string{".haraqa-queue1", ".haraqa-queue2"}
	// clean volumes before/after
	for i := range volumes {
		_ = os.RemoveAll(volumes[i])
	}
	defer func() {
		for i := range volumes {
			_ = os.RemoveAll(volumes[i])
		}
	}()

	// new queue
	maxEntries := 10
	consumePoolSize := uint64(1)

	q, err := NewQueue(volumes, maxEntries, consumePoolSize)
	if err != nil {
		t.Fatal(err)
	}
	topic := []byte("queue-topic")

	t.Run("create", testCreate(q, topic))
	t.Run("list", testList(q, topic))
	t.Run("produce consume", testProduceConsume(q, topic))
	t.Run("offests", testOffsets(q, topic))

	_ = os.RemoveAll(volumes[0])
	_, err = NewQueue(volumes, maxEntries, consumePoolSize)
	if err != nil {
		t.Fatal(err)
	}

	err = q.DeleteTopic(topic)
	if err != nil {
		t.Fatal(err)
	}
}

func testCreate(q Queue, topic []byte) func(*testing.T) {
	return func(t *testing.T) {
		err := q.CreateTopic(topic)
		if err != nil {
			t.Fatal(err)
		}
		err = q.CreateTopic(topic)
		if err != protocol.ErrTopicExists {
			t.Fatal(err)
		}

		delete(q.(*queue).produceTopics, string(topic))
		err = q.CreateTopic(topic)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func testList(q Queue, topic []byte) func(*testing.T) {
	return func(t *testing.T) {
		// list topics
		topics, err := q.ListTopics("", "", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(topics) != 1 || !bytes.Equal(topic, topics[0]) {
			t.Fatal(topics)
		}

		// list w/prefix
		topics, err = q.ListTopics(string(topic[:3]), "", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(topics) != 1 || !bytes.Equal(topic, topics[0]) {
			t.Fatal(topics)
		}

		// list w/invalid prefix
		topics, err = q.ListTopics("invalid", "", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(topics) != 0 {
			t.Fatal(topics)
		}

		// list w/suffix
		topics, err = q.ListTopics("", string(topic[len(topic)-4:]), "")
		if err != nil {
			t.Fatal(err)
		}
		if len(topics) != 1 || !bytes.Equal(topic, topics[0]) {
			t.Fatal(topics)
		}

		// list w/invalid suffix
		topics, err = q.ListTopics("", "invalid", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(topics) != 0 {
			t.Fatal(topics)
		}

		// list w/regex
		topics, err = q.ListTopics("", "", ".*")
		if err != nil {
			t.Fatal(err)
		}
		if len(topics) != 1 || !bytes.Equal(topic, topics[0]) {
			t.Fatal(topics)
		}

		// list w/invalid regex
		_, err = q.ListTopics("", "", `\`)
		if err == nil {
			t.Fatal(err)
		}

		// list w/invalid regex
		topics, err = q.ListTopics("", "", ".*blue")
		if err != nil {
			t.Fatal(err)
		}
		if len(topics) != 0 {
			t.Fatal(topics)
		}
	}
}

func testProduceConsume(q Queue, topic []byte) func(*testing.T) {
	return func(t *testing.T) {
		err := q.Produce(nil, nil, nil)
		if err != protocol.ErrTopicDoesNotExist {
			t.Fatal(err)
		}

		_, _, _, err = q.ConsumeInfo(nil, 0, 0)
		if !os.IsNotExist(errors.Cause(err)) {
			t.Fatal(err)
		}
		_, _, _, err = q.ConsumeInfo([]byte("invalid"), 0, 0)
		if errors.Cause(err) != protocol.ErrTopicDoesNotExist {
			t.Fatal(err)
		}
		err = q.Consume(nil, nil, nil, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		err = q.Consume(nil, nil, nil, 0, 10)
		if !os.IsNotExist(errors.Cause(err)) {
			t.Fatal(err)
		}

		l, err := net.Listen("tcp", "0.0.0.0:0")
		if err != nil {
			t.Fatal(err)
		}

		errs := make(chan error, 1)
		go func() {
			conn, err := net.Dial(l.Addr().Network(), l.Addr().String())
			if err != nil {
				t.Log(err)
				errs <- err
				return
			}
			in := []byte("hello world")
			_, err = conn.Write(in)
			if err != nil {
				t.Log(err)
				errs <- err
				return
			}
			out := make([]byte, 11)
			_, err = conn.Read(out)
			if err != nil {
				t.Log(err)
				errs <- err
				return
			}
			if !bytes.Equal(in, out) {
				t.Log(string(in), string(out))
				errs <- errors.New("invalid output")
				return
			}
			errs <- nil
		}()
		conn, err := l.Accept()
		if err != nil {
			t.Fatal(err)
		}
		tcpConn, err := conn.(*net.TCPConn).File()
		if err != nil {
			t.Fatal(err)
		}

		// remove topic from produce map
		q.(*queue).produceTopics[string(topic)] = nil

		msgSizes := []int64{5, 6}
		err = q.Produce(tcpConn, topic, msgSizes)
		if err != nil {
			t.Fatal(err)
		}

		filename, startAt, sizes, err := q.ConsumeInfo(topic, 0, 2)
		if err != nil {
			t.Fatal(err)
		}
		var totalSize int64
		for i := range sizes {
			totalSize += sizes[i]
		}
		err = q.Consume(tcpConn, topic, filename, startAt, totalSize)
		if err != nil {
			t.Fatal(err)
		}
		err = <-errs
		if err != nil {
			t.Fatal(err)
		}
	}
}

func testOffsets(q Queue, topic []byte) func(*testing.T) {
	return func(t *testing.T) {
		// non-existing topic
		_, _, err := q.Offsets([]byte("invalid"))
		if err != os.ErrNotExist {
			t.Fatal(err)
		}

		// existing folder, non-existing topic
		vol := q.(*queue).volumes
		dir := filepath.Join(vol[len(vol)-1], "emptydir")
		err = os.MkdirAll(dir, 0777)
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = q.Offsets([]byte("emptydir"))
		if err != os.ErrNotExist {
			t.Fatal(err)
		}
		_, err = os.OpenFile(filepath.Join(dir, "invalid.dat"), os.O_CREATE, 0777)
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = q.Offsets([]byte("emptydir"))
		if err != os.ErrNotExist {
			t.Fatal(err)
		}

		// new topic
		newTopic := []byte("offsets-topic")
		err = q.CreateTopic(newTopic)
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = q.Offsets(newTopic)
		if err != os.ErrNotExist {
			t.Fatal(err)
		}

		// populated topic
		min, max, err := q.Offsets(topic)
		if err != nil {
			t.Fatal(err)
		}
		if min != 0 || max != 2 {
			t.Fatal(min, max)
		}
	}
}

func TestTruncateTopic(t *testing.T) {
	err := os.MkdirAll(".haraqa-truncate/truncate_topic/subdir", os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	q := &queue{
		volumes: []string{".haraqa-truncate"},
	}
	topic := "truncate_topic"

	// setup files
	f, err := os.Create(filepath.Join(q.volumes[0], topic, formatFilename(0)+datFileExt))
	if err != nil {
		t.Fatal(err)
	}
	f.Sync()
	f.Close()
	f, err = os.Create(filepath.Join(q.volumes[0], topic, formatFilename(0)+hrqFileExt))
	if err != nil {
		t.Fatal(err)
	}
	f.Sync()
	f.Close()
	f, err = os.Create(filepath.Join(q.volumes[0], topic, formatFilename(1000)+datFileExt))
	if err != nil {
		t.Fatal(err)
	}
	f.Sync()
	f.Close()
	f, err = os.Create(filepath.Join(q.volumes[0], topic, formatFilename(1000)+hrqFileExt))
	if err != nil {
		t.Fatal(err)
	}
	f.Sync()
	f.Close()

	before := time.Now()
	err = q.TruncateTopic([]byte(topic), 0, before)
	if err != nil {
		t.Fatal(err)
	}
	zeroTime := time.Time{}
	err = q.TruncateTopic([]byte(topic), 0, zeroTime)
	if err != nil {
		t.Fatal(err)
	}
	err = q.TruncateTopic([]byte(topic), -1, zeroTime)
	if err != nil {
		t.Fatal(err)
	}
	err = q.TruncateTopic([]byte(topic), 20, zeroTime)
	if err != nil {
		t.Fatal(err)
	}
	err = q.TruncateTopic([]byte(topic), 2000, zeroTime)
	if err != nil {
		t.Fatal(err)
	}

	// verify last still exists
	f, err = os.Open(filepath.Join(q.volumes[0], topic, formatFilename(1000)+datFileExt))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	f, err = os.Open(filepath.Join(q.volumes[0], topic, formatFilename(1000)+hrqFileExt))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
}
