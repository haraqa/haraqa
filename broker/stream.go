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

type dataTriggers struct {
	sync.Mutex

	m map[string]chan dataTrigger
}

type dataTrigger struct {
	incoming  bool
	topic     []byte
	sizes     []int64
	filename  []byte
	startAt   int64
	totalSize int64
}

func (b *Broker) newDataTriggerChannel(id []byte) chan dataTrigger {
	b.dataTriggers.Lock()
	old, ok := b.dataTriggers.m[string(id)]
	if ok {
		close(old)
	}
	ch := make(chan dataTrigger, 1)
	b.dataTriggers.m[string(id)] = ch
	b.dataTriggers.Unlock()
	return ch
}

func (b *Broker) sendToDataTriggerChannel(id []byte, s dataTrigger) bool {
	b.dataTriggers.Lock()
	ch, ok := b.dataTriggers.m[string(id)]
	if ok {
		ch <- s
	}
	b.dataTriggers.Unlock()
	return ok
}

func (b *Broker) closeDataTriggerChannel(id []byte) {
	b.dataTriggers.Lock()
	ch, ok := b.dataTriggers.m[string(id)]
	if ok {
		close(ch)
	}
	delete(b.dataTriggers.m, string(id))
	b.dataTriggers.Unlock()
	return
}

type netConn interface {
	net.Conn
	File() (*os.File, error)
}

func (b *Broker) handleDataConn(c netConn) {
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
		log.Println(errors.Wrapf(err, "unable to read data connection id %v", id[:]))
		return
	}

	_, err = conn.Write([]byte{0, 0})
	if err != nil {
		log.Println(errors.Wrap(err, "unable to write data connection confirmation"))
		return
	}

	ch := b.newDataTriggerChannel(id[:])
	var d dataTrigger
	var resp [2]byte
	for d = range ch {
		if d.incoming {
			// client is producing
			err = b.config.Queue.Produce(conn, d.topic, d.sizes)
			resp = protocol.ErrorToResponse(err)
			_, err2 := conn.Write(resp[:])
			if err != nil {
				log.Println(errors.Wrap(err, "unable to read produce from data connection"))
				return
			}
			if err2 != nil {
				log.Println(errors.Wrap(err2, "unable to send produce response to data connection"))
				return
			}
			continue
		}

		// client is consuming
		err = b.config.Queue.Consume(conn, d.topic, d.filename, d.startAt, d.totalSize)
		if err != nil {
			log.Println(errors.Wrap(err, "unable to process consume request on data connection"))
			return
		}
	}
}
