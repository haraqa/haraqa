package broker

import (
	"io"
	"log"
	"net"
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

func (b *Broker) getStreamChannel(id []byte) chan stream {
	b.streams.Lock()
	ch, ok := b.streams.m[string(id)]
	if !ok {
		ch = make(chan stream, 1)
		b.streams.m[string(id)] = ch
	}
	b.streams.Unlock()
	return ch
}

func (b *Broker) handleStream(c *net.TCPConn) {
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

	ch := b.getStreamChannel(id[:])
	var s stream
	var resp [2]byte
	for s = range ch {
		if s.incoming {
			// client is producing
			err = b.config.Queue.Produce(conn, s.topic, s.sizes)
			resp = protocol.ErrorToResponse(err)
			conn.Write(resp[:])
			if err != nil {
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
