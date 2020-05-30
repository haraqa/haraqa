package queue

import (
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/haraqa/haraqa/internal/protocol"
	"github.com/pkg/errors"
)

func newMockTCPConn(t *testing.T, buf []byte) *os.File {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		conn, err := net.Dial(l.Addr().Network(), l.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_, err = conn.Write(buf)
		if err != nil {
			t.Fatal(err)
		}
	}()
	conn, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	f, err := conn.(*net.TCPConn).File()
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestProduce(t *testing.T) {
	volumes := []string{".haraqa-0", ".haraqa-1", ".haraqa-2"}
	maxEntries := 10
	consumePoolSize := uint64(10)
	topic := []byte("produce_topic")
	msgSizes := make([]int64, 50)
	for i := range msgSizes {
		msgSizes[i] = int64(i + 20)
	}
	buf := make([]byte, sum(msgSizes))
	rand.Read(buf)
	tcpConn := newMockTCPConn(t, buf)

	Q, err := NewQueue(volumes, maxEntries, consumePoolSize)
	if err != nil {
		t.Fatal(err)
	}
	q := Q.(*queue)

	// should fail with no topic
	err = q.Produce(tcpConn, topic, nil)
	if errors.Cause(err) != protocol.ErrTopicDoesNotExist {
		t.Fatal(err)
	}

	err = q.CreateTopic(topic)
	if err != nil {
		t.Fatal(err)
	}

	// file 0 should have 11 messages
	checkProduce(t, q, tcpConn, topic, msgSizes[:11], 11, 11)

	// file 11 should have 9 messages
	checkProduce(t, q, tcpConn, topic, msgSizes[11:20], 9, 20)

	// file 20 should have 5 messages, then 9, then 10
	checkProduce(t, q, tcpConn, topic, msgSizes[20:25], 5, 25)
	checkProduce(t, q, tcpConn, topic, msgSizes[25:29], 9, 29)
	checkProduce(t, q, tcpConn, topic, msgSizes[29:30], 10, 30)

	// file 30 should have 10 messages
	checkProduce(t, q, tcpConn, topic, msgSizes[30:40], 10, 40)

	fmt.Println("new queue")

	Q2, err := NewQueue(volumes, maxEntries, consumePoolSize)
	if err != nil {
		t.Fatal(err)
	}
	q2 := Q2.(*queue)

	// file 40 should have 5 messages
	checkProduce(t, q2, tcpConn, topic, msgSizes[40:45], 5, 45)

	Q3, err := NewQueue(volumes, maxEntries, consumePoolSize)
	if err != nil {
		t.Fatal(err)
	}
	q3 := Q3.(*queue)

	// file 40 should have 10 messages
	checkProduce(t, q3, tcpConn, topic, msgSizes[45:50], 10, 50)
}

func checkProduce(t *testing.T, q *queue, tcpConn *os.File, topic []byte, msgSizes []int64, relOffset, globalOffset int64) {
	err := q.Produce(tcpConn, topic, msgSizes)
	if err != nil {
		t.Fatal(err)
	}
	pfs := q.produceFileSets[string(topic)]
	if pfs.relOffset != relOffset {
		t.Fatalf("expected relative offset to be %d, got %d", relOffset, pfs.relOffset)
	}
	if pfs.globalOffset != globalOffset {
		t.Fatalf("expected global offset to be %d, got %d", globalOffset, pfs.globalOffset)
	}
}
