package broker

import (
	"io"
	"log"
	"net"
	"os"
	"sync"

	"github.com/haraqa/haraqa/protocol"
	"github.com/pkg/errors"
)

type streams struct {
	sync.Mutex

	m map[string]chan stream
}

type stream struct {
	incoming  bool
	topic     []byte
	sizes     []int64
	filename  []byte
	startAt   int64
	totalSize int64
}

func (b *Broker) newStreamChannel(id []byte) chan stream {
	b.streams.Lock()
	old, ok := b.streams.m[string(id)]
	if ok {
		close(old)
	}
	ch := make(chan stream, 1)
	b.streams.m[string(id)] = ch
	b.streams.Unlock()
	return ch
}

func (b *Broker) sendToStreamChannel(id []byte, s stream) bool {
	b.streams.Lock()
	ch, ok := b.streams.m[string(id)]
	if ok {
		ch <- s
	}
	b.streams.Unlock()
	return ok
}

func (b *Broker) closeStreamChannel(id []byte) {
	b.streams.Lock()
	ch, ok := b.streams.m[string(id)]
	if ok {
		close(ch)
	}
	delete(b.streams.m, string(id))
	b.streams.Unlock()
	return
}

type netConn interface {
	net.Conn
	File() (*os.File, error)
}

func (b *Broker) handleStream(c netConn) {
	conn, err := c.File()
	c.Close()
	if err != nil {
		log.Println(errors.Wrap(err, "unable to get tcp connection file"))
		return
	}
	defer conn.Close()

	var id [16]byte
	_, err = io.ReadFull(conn, id[:])
	if err != nil {
		log.Println(errors.Wrap(err, "unable to read stream id"))
		return
	}

	_, err = conn.Write([]byte{0, 0})
	if err != nil {
		log.Println(errors.Wrap(err, "unable to write stream confirmation"))
		return
	}

	ch := b.newStreamChannel(id[:])
	var s stream
	var resp [2]byte
	for s = range ch {
		if s.incoming {
			// client is producing
			err = b.config.Queue.Produce(conn, s.topic, s.sizes)
			resp = protocol.ErrorToResponse(err)
			_, err2 := conn.Write(resp[:])
			if err != nil {
				log.Println(err)
				return
			}
			if err2 != nil {
				log.Println(err)
				return
			}
			continue
		}

		// client is consuming
		err = b.config.Queue.Consume(conn, s.topic, s.filename, s.startAt, s.totalSize)
		if err != nil {
			log.Println(err)
			return
		}
	}
}
