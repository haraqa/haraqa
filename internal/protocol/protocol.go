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
	binary.BigEndian.PutUint32(b[2:6], uint32(len(b)-6))
	copy(b[6:], []byte(msg))
	// best effort write the response to the connection
	_, _ = conn.Write(b)
}

// each data message begins with 6 bytes
// 0-2: message flags/type
// 2-6: header length

// Types for messages incoming over a data connection
const (
	TypeError byte = iota + 1
	TypePing
	TypeProduce
	TypeConsume
	TypeClose
)

// ExtendBuffer increases a buffer's length if needed and returns a buffer of length n
func ExtendBuffer(buf *[]byte, n int) {
	if n > cap(*buf) {
		*buf = append(*buf, make([]byte, n-len(*buf))...)
	}
	*buf = (*buf)[:n]
}

// ReadPrefix reads the first 6 bytes of a data connection into the prefix and
//  interprets any response errors accordingly
func ReadPrefix(conn io.Reader, prefix []byte) (byte, uint32, error) {
	if len(prefix) != 6 {
		return 0, 0, errors.New("invalid prefix length")
	}
	_, err := io.ReadFull(conn, prefix)
	if err != nil {
		return 0, 0, errors.Wrap(err, "unable to read prefix")
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
			errMsg := string(e[:n])
			switch errMsg {
			case ErrTopicExists.Error():
				return prefix[1], hLength, ErrTopicExists
			case ErrTopicDoesNotExist.Error():
				return prefix[1], hLength, ErrTopicDoesNotExist
			default:
				return prefix[1], hLength, errors.New(errMsg)
			}
		}
		return prefix[1], hLength, err

	default:
		return 0, 0, errors.Errorf("invalid type %v", prefix)
	}
}

// ProduceRequest is a wrapper for the binary protocol for a produce message over a
//  data connection
type ProduceRequest struct {
	Topic    []byte
	MsgSizes []int64
}

// Read is used in the broker for a ProduceRequest, it reads the given buffer and
//  populates the ProduceRequest's Topic and MsgSizes fields
func (p *ProduceRequest) Read(b []byte) error {
	if len(b) < 2 {
		return errors.New("invalid produce header length")
	}
	topicLength := binary.BigEndian.Uint16(b[:2])
	if int(topicLength) > len(b)-2 {
		return errors.New("invalid topic length")
	}
	n := 2

	// read into topic
	ExtendBuffer(&p.Topic, int(topicLength))
	n += copy(p.Topic, b[n:])

	// check valid length
	if len(b[n:])%8 != 0 {
		return errors.New("invalid messages length")
	}

	// read into message lengths
	count := len(b[n:]) / 8
	if count > cap(p.MsgSizes) {
		p.MsgSizes = append(p.MsgSizes, make([]int64, count-len(p.MsgSizes))...)
	}
	p.MsgSizes = p.MsgSizes[:count]

	for i := range p.MsgSizes {
		p.MsgSizes[i] = int64(binary.BigEndian.Uint64(b[n : n+8]))
		n += 8
	}
	return nil
}

// Write is called by the client to serialize a Produce request and write it to the connection
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

// ConsumeRequest is the request made to the broker by the client for a batch of messages from a topic
// starting at a given offset
type ConsumeRequest struct {
	Topic  []byte
	Offset int64
	Limit  int64
}

func (c *ConsumeRequest) Read(b []byte) error {
	if len(b) < 2 {
		return errors.New("invalid consume header length")
	}
	topicLength := binary.BigEndian.Uint16(b[:2])
	if int(topicLength) != len(b)-18 {
		return errors.New("invalid topic length")
	}
	n := 2

	// read into topic
	ExtendBuffer(&c.Topic, int(topicLength))
	n += copy(c.Topic, b[n:])

	// read offset
	c.Offset = int64(binary.BigEndian.Uint64(b[n : n+8]))
	n += 8
	c.Limit = int64(binary.BigEndian.Uint64(b[n : n+8]))
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
	binary.BigEndian.PutUint64(buf[n:n+8], uint64(c.Limit))

	_, err := conn.Write(buf)
	return err
}

// ConsumeResponse is the message from a broker to a client notifying of messages
//  being written to the data cannection and their respective sizes
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
	if count > cap(p.MsgSizes) {
		p.MsgSizes = append(p.MsgSizes, make([]int64, count-len(p.MsgSizes))...)
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
