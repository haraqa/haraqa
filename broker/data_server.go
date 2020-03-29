package broker

import (
	"io"
	"log"
	"net"
	"os"

	"github.com/haraqa/haraqa/internal/protocol"
	"github.com/pkg/errors"
)

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

	var prefix [6]byte
	var produceReq protocol.ProduceRequest
	var consumeReq protocol.ConsumeRequest
	var consumeResp protocol.ConsumeResponse
	var buf []byte
	for {
		// read prefix
		t, hLen, err := protocol.ReadPrefix(conn, prefix[:])
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return
			}
			log.Println("read prefix error:", err)
			protocol.ErrorToResponse(conn, err)
			return
		}

		switch t {
		case protocol.TypeClose:
			log.Println("Closing client connection")
			return

		case protocol.TypePing:
			_, err = conn.Write(prefix[:])
			if err != nil {
				log.Println("ping message write resp error:", err)
				protocol.ErrorToResponse(conn, err)
				return
			}

		case protocol.TypeProduce:
			err = b.handleProduce(conn, &produceReq, &buf, hLen, prefix[:])
			if err != nil {
				log.Println(err)
				return
			}

		case protocol.TypeConsume:
			err = b.handleConsume(conn, &consumeReq, &consumeResp, &buf, hLen, prefix[:])
			if err != nil {
				log.Println(err)
				return
			}
		}
	}
}

func (b *Broker) handleProduce(conn *os.File, produceReq *protocol.ProduceRequest, buf *[]byte, hLen uint32, prefix []byte) error {
	// read header
	protocol.ExtendBuffer(buf, int(hLen))
	_, err := io.ReadFull(conn, *buf)
	if err != nil {
		protocol.ErrorToResponse(conn, err)
		return errors.Wrap(err, "read produce header buffer error")
	}

	// read header
	err = produceReq.Read(*buf)
	if err != nil {
		protocol.ErrorToResponse(conn, err)
		return errors.Wrap(err, "produce message read error")
	}

	// check msg sizes
	if b.config.MaxSize > 0 {
		for i := range produceReq.MsgSizes {
			if produceReq.MsgSizes[i] > b.config.MaxSize {
				err = errors.Errorf("invalid message size. exceeds maximum limit of %d bytes", b.config.MaxSize)
				protocol.ErrorToResponse(conn, err)
				return err
			}
		}
	}

	// write to queue
	err = b.Q.Produce(conn, produceReq.Topic, produceReq.MsgSizes)
	if err != nil {
		protocol.ErrorToResponse(conn, err)
		return errors.Wrap(err, "produce message queue error")
	}

	// write response
	prefix[2], prefix[3] = 0, 0
	_, err = conn.Write(prefix[:])
	if err != nil {
		protocol.ErrorToResponse(conn, err)
		return errors.Wrap(err, "produce message write resp error")
	}
	return nil
}

func (b *Broker) handleConsume(conn *os.File, consumeReq *protocol.ConsumeRequest, consumeResp *protocol.ConsumeResponse, buf *[]byte, hLen uint32, prefix []byte) error {
	// read header
	protocol.ExtendBuffer(buf, int(hLen))
	_, err := io.ReadFull(conn, *buf)
	if err != nil {
		log.Println("read header buffer error:", err)
		protocol.ErrorToResponse(conn, err)
		return errors.Wrap(err, "read consume header buffer error")
	}

	// read header
	err = consumeReq.Read(*buf)
	if err != nil {
		protocol.ErrorToResponse(conn, err)
		return errors.Wrap(err, "consume message read error")
	}

	// read consume metadata
	filename, startAt, msgSizes, err := b.Q.ConsumeInfo(consumeReq.Topic, consumeReq.Offset, consumeReq.Limit)
	if err != nil {
		protocol.ErrorToResponse(conn, err)
		return errors.Wrap(err, "consume info error")
	}

	// setup consume header
	consumeResp.MsgSizes = msgSizes
	err = consumeResp.Write(conn)
	if err != nil {
		protocol.ErrorToResponse(conn, err)
		return errors.Wrap(err, "consume write header error")
	}

	// write consume body
	var totalSize int64
	for i := range msgSizes {
		totalSize += msgSizes[i]
	}
	err = b.Q.Consume(conn, consumeReq.Topic, filename, startAt, totalSize)
	if err != nil {
		return errors.Wrap(err, "consume write error")
	}
	return nil
}
