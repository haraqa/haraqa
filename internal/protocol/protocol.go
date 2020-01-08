package protocol

import (
	"encoding/binary"
	io "io"

	"github.com/pkg/errors"
)

//go:generate protoc --gogofaster_out=plugins=grpc:. grpc.proto

var (
	//ErrTopicExists is returned if a CreateTopic request is made to an existing topic
	ErrTopicExists = errors.New("topic already exists")
	//ErrTopicDoesNotExist is returned if a Request is made on a non existent topic
	ErrTopicDoesNotExist = errors.New("topic does not exist")
)

// ErrorToResponse converts a standard error to a response for data connection responses
func ErrorToResponse(conn io.Writer, err error) {
	msg := errors.Cause(err).Error()
	b := make([]byte, 6+len(msg))
	b[0], b[1] = 0, TypeError
	binary.BigEndian.PutUint32(b[2:6], uint32(len(b)-4))
	copy(b[4:], []byte(msg))
	conn.Write(b)
	return
}

// each data message begins with 6 bytes
// 0-2: message flags/type
// 2-6: header length

const (
	TypeError byte = iota + 1
	TypePing
	TypeProduce
	TypeConsume
	TypeClose
)

func ExtendBuffer(buf *[]byte, n int) {
	if cap(*buf) >= n {
		*buf = (*buf)[:n]
		return
	}

	if len(*buf) < n {
		*buf = append(*buf, make([]byte, n-len(*buf))...)
	}
	*buf = (*buf)[:n]
}

func ReadPrefix(conn io.Reader, prefix []byte) (byte, uint32, error) {
	if len(prefix) != 6 {
		return 0, 0, errors.New("invalid prefix length")
	}
	_, err := io.ReadFull(conn, prefix)
	if err != nil {
		return 0, 0, err
	}

	if prefix[0] != 0 {
		return 0, 0, errors.Errorf("invalid type %v", prefix)
	}
	switch prefix[1] {
	case TypePing, TypeClose:
		return prefix[1], 0, nil
	case TypeProduce, TypeConsume:
		hLength := binary.BigEndian.Uint32(prefix[2:6])
		return prefix[1], hLength, nil
	case TypeError:
		hLength := binary.BigEndian.Uint32(prefix[2:6])
		e := make([]byte, hLength)
		n, err := io.ReadFull(conn, e)
		if err == nil || n > 0 {
			return prefix[1], hLength, errors.New(string(e[:n]))
		}
		return prefix[1], hLength, err

	default:
		return 0, 0, errors.Errorf("invalid type %v", prefix)
	}
}

type ProduceRequest struct {
	Topic    []byte
	MsgSizes []int64
}

func (p *ProduceRequest) Read(b []byte) error {
	topicLength := binary.BigEndian.Uint16(b[:2])
	if int(topicLength) > len(b)-2 {
		return errors.New("invalid topic length")
	}
	n := 2

	// read into topic
	if len(p.Topic) < int(topicLength) {
		p.Topic = make([]byte, topicLength)
	}
	p.Topic = p.Topic[:topicLength]
	n += copy(p.Topic, b[n:])

	// check valid length
	if len(b[n:])%8 != 0 {
		return errors.New("invalid messages length")
	}

	// read into message lengths
	count := len(b[n:]) / 8
	if count > len(p.MsgSizes) {
		p.MsgSizes = make([]int64, count)
	}
	p.MsgSizes = p.MsgSizes[:count]

	for i := range p.MsgSizes {
		p.MsgSizes[i] = int64(binary.BigEndian.Uint64(b[n : n+8]))
		n += 8
	}
	return nil
}

func (p *ProduceRequest) Write(conn io.Writer) error {
	headerLength := 2 + len(p.Topic) + len(p.MsgSizes)*8
	buf := make([]byte, 6+headerLength)
	// type
	buf[0], buf[1] = 0, TypeProduce
	n := 2

	// header length
	binary.BigEndian.PutUint32(buf[n:n+4], uint32(headerLength))
	n += 4

	// topic length
	binary.BigEndian.PutUint16(buf[n:n+2], uint16(len(p.Topic)))
	n += 2

	// topic
	n += copy(buf[n:n+len(p.Topic)], p.Topic[:uint16(len(p.Topic))])

	// message lengths
	for i := range p.MsgSizes {
		binary.BigEndian.PutUint64(buf[n:n+8], uint64(p.MsgSizes[i]))
		n += 8
	}

	_, err := conn.Write(buf)
	return err
}

type ConsumeRequest struct {
	Topic        []byte
	Offset       int64
	MaxBatchSize int64
}

func (c *ConsumeRequest) Read(b []byte) error {
	topicLength := binary.BigEndian.Uint16(b[:2])
	if int(topicLength) != len(b)-18 {
		return errors.New("invalid topic length")
	}
	n := 2

	// read into topic
	if len(c.Topic) < int(topicLength) {
		c.Topic = make([]byte, topicLength)
	}
	c.Topic = c.Topic[:topicLength]
	n += copy(c.Topic, b[n:])

	// read offset
	c.Offset = int64(binary.BigEndian.Uint64(b[n : n+8]))
	n += 8
	c.MaxBatchSize = int64(binary.BigEndian.Uint64(b[n : n+8]))
	return nil
}

func (c *ConsumeRequest) Write(conn io.Writer) error {
	headerLength := 2 + len(c.Topic) + 16
	buf := make([]byte, 6+headerLength)
	// type
	buf[0], buf[1] = 0, TypeConsume
	n := 2

	// header length
	binary.BigEndian.PutUint32(buf[n:n+4], uint32(headerLength))
	n += 4

	// topic length
	binary.BigEndian.PutUint16(buf[n:n+2], uint16(len(c.Topic)))
	n += 2

	//topic
	n += copy(buf[n:], c.Topic)

	// offset
	binary.BigEndian.PutUint64(buf[n:n+8], uint64(c.Offset))
	n += 8

	// max batch size
	binary.BigEndian.PutUint64(buf[n:n+8], uint64(c.MaxBatchSize))

	_, err := conn.Write(buf)
	return err
}

type ConsumeResponse struct {
	MsgSizes []int64
}

func (p *ConsumeResponse) Read(b []byte) error {
	// check valid length
	if len(b)%8 != 0 {
		return errors.New("invalid messages length")
	}

	// read into message lengths
	count := len(b) / 8
	if count > len(p.MsgSizes) {
		p.MsgSizes = make([]int64, count)
	}
	p.MsgSizes = p.MsgSizes[:count]
	n := 0
	for i := range p.MsgSizes {
		p.MsgSizes[i] = int64(binary.BigEndian.Uint64(b[n : n+8]))
		n += 8
	}
	return nil
}

func (p *ConsumeResponse) Write(conn io.Writer) error {
	headerLength := len(p.MsgSizes) * 8
	buf := make([]byte, 6+headerLength)
	// type
	buf[0], buf[1] = 0, TypeConsume
	n := 2

	// header length
	binary.BigEndian.PutUint32(buf[n:n+4], uint32(headerLength))
	n += 4

	// message lengths
	for i := range p.MsgSizes {
		binary.BigEndian.PutUint64(buf[n:n+8], uint64(p.MsgSizes[i]))
		n += 8
	}

	_, err := conn.Write(buf)
	return err
}
