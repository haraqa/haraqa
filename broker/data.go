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

	var prefix [4]byte
	var produceReq protocol.ProduceRequest
	var consumeReq protocol.ConsumeRequest
	var consumeResp protocol.ConsumeResponse
	var buf []byte
	for {
		// read prefix
		t, hLen, err := protocol.ReadPrefix(conn, prefix[:])
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Println("read prefix error:", err)
			protocol.ErrorToResponse(conn, err)
			return
		}

		if t == protocol.TypeClose {
			return
		}
		if t == protocol.TypePing {
			_, err = conn.Write(prefix[:])
			if err != nil {
				log.Println("ping message write resp error:", err)
				protocol.ErrorToResponse(conn, err)
				return
			}
			continue
		}

		// read header
		protocol.ExtendBuffer(&buf, int(hLen))
		_, err = io.ReadFull(conn, buf)
		if err != nil {
			log.Println("read header buffer error:", err)
			protocol.ErrorToResponse(conn, err)
			return
		}

		switch t {
		case protocol.TypeProduce:
			// read header
			err = produceReq.Read(buf)
			if err != nil {
				log.Println("produce message read error:", err)
				protocol.ErrorToResponse(conn, err)
				return
			}

			// write to queue
			err = b.config.Queue.Produce(conn, produceReq.Topic, produceReq.MsgSizes)
			if err != nil {
				log.Println("produce message queue error:", err)
				protocol.ErrorToResponse(conn, err)
				return
			}

			// write response
			prefix[2], prefix[3] = 0, 0
			_, err = conn.Write(prefix[:])
			if err != nil {
				log.Println("produce message write resp error:", err)
				protocol.ErrorToResponse(conn, err)
				return
			}

		case protocol.TypeConsume:
			// read header
			err = consumeReq.Read(buf)
			if err != nil {
				log.Println("consume message read error:", err)
				protocol.ErrorToResponse(conn, err)
				return
			}

			// read consume metadata
			filename, startAt, msgSizes, err := b.config.Queue.ConsumeInfo(consumeReq.Topic, consumeReq.Offset, consumeReq.MaxBatchSize)
			if err != nil {
				log.Println("consume info error:", err)
				protocol.ErrorToResponse(conn, err)
				return
			}

			// setup consume header
			consumeResp.MsgSizes = msgSizes
			err = consumeResp.Write(conn)
			if err != nil {
				log.Println("consume write header error:", err)
				protocol.ErrorToResponse(conn, err)
				return
			}

			// write consume body
			var totalSize int64
			for i := range msgSizes {
				totalSize += msgSizes[i]
			}
			err = b.config.Queue.Consume(conn, consumeReq.Topic, filename, startAt, totalSize)
			if err != nil {
				log.Println("consume write error:", err)
				return
			}
		}
	}
}
