package haraqa

import (
	"context"
	"crypto/rand"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/haraqa/haraqa/protocol"
	pb "github.com/haraqa/haraqa/protocol"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var DefaultConfig = Config{
	Host:         "127.0.0.1",
	GRPCPort:     4353,
	StreamPort:   14353,
	CreateTopics: true,
	Timeout:      time.Second * 5,
}

type Config struct {
	Host         string
	GRPCPort     int
	StreamPort   int
	CreateTopics bool
	UnixSocket   string
	Timeout      time.Duration
}

type Client struct {
	config     Config
	id         [16]byte
	conn       *grpc.ClientConn
	client     pb.HaraqaClient
	streamLock sync.Mutex
	stream     net.Conn
	streamBuf  []byte
}

// NewClient creates a new haraqa client based on the given config
//  cfg := haraqa.DefaultConfig
//  haraqa.NewClient(cfg)
func NewClient(config Config) (*Client, error) {
	if config.Host == "" {
		return nil, errors.New("invalid host")
	}
	if config.GRPCPort == 0 || (config.UnixSocket == "" && config.StreamPort == 0) {
		return nil, errors.New("invalid ports")
	}

	// Set up a connection to the server.
	conn, err := grpc.Dial(config.Host+":"+strconv.Itoa(config.GRPCPort), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, errors.Wrapf(err, "unable to connect to grpc port %q", config.Host+":"+strconv.Itoa(config.GRPCPort))
	}
	client := pb.NewHaraqaClient(conn)
	if client == nil {
		return nil, errors.New("unable to create new grpc client")
	}

	// generate a new id
	var id [16]byte
	_, err = io.ReadFull(rand.Reader, id[:])
	if err != nil {
		return nil, errors.Wrap(err, "unable to generate client id")
	}
	c := &Client{
		config: config,
		conn:   conn,
		client: client,
		id:     id,
	}

	err = c.streamConnect()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Client) Close() error {
	err := c.conn.Close()
	err2 := c.stream.Close()
	if err != nil {
		return errors.Wrap(err, "unable to close grpc connection")
	}
	if err2 != nil {
		return errors.Wrap(err2, "unable to close stream connection")
	}
	return nil
}

func (c *Client) streamConnect() error {
	if c.stream != nil {
		return nil
	}
	var err error
	var stream net.Conn
	// connect to streaming port
	if c.config.UnixSocket != "" {
		stream, err = net.Dial("unix", c.config.UnixSocket)
		if err != nil {
			return errors.Wrapf(err, "unable to connect to unix socket %q", c.config.UnixSocket)
		}
	} else {
		stream, err = net.Dial("tcp", c.config.Host+":"+strconv.Itoa(c.config.StreamPort))
		if err != nil {
			return errors.Wrapf(err, "unable to connect to stream port %q", c.config.Host+":"+strconv.Itoa(c.config.StreamPort))
		}
	}

	// initialize stream with id
	_, err = stream.Write(c.id[:])
	if err != nil {
		return errors.Wrap(err, "unable write to stream port")
	}

	var resp [2]byte
	_, err = io.ReadFull(stream, resp[:])
	if err != nil {
		return errors.Wrap(err, "unable read from stream port")
	}
	c.stream = stream
	return nil
}

func (c *Client) CreateTopic(ctx context.Context, topic []byte) error {
	ctx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()

	// send message metadata
	r, err := c.client.CreateTopic(ctx, &pb.CreateTopicRequest{
		Topic: topic,
	})
	if err != nil {
		return errors.Wrap(err, "could not produce")
	}
	meta := r.GetMeta()
	if !meta.GetOK() {
		return errors.Wrapf(errors.New(meta.GetErrorMsg()), "broker error creating topic %q", string(topic))
	}
	return nil
}

func (c *Client) DeleteTopic(ctx context.Context, topic []byte) error {
	ctx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()

	// send message metadata
	r, err := c.client.DeleteTopic(ctx, &pb.DeleteTopicRequest{
		Topic: topic,
	})
	if err != nil {
		return errors.Wrap(err, "could not produce")
	}
	meta := r.GetMeta()
	if !meta.GetOK() {
		return errors.Wrapf(errors.New(meta.GetErrorMsg()), "broker error deleting topic %q", string(topic))
	}
	return nil
}

func (c *Client) ListTopics(ctx context.Context) ([][]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()

	// send message metadata
	r, err := c.client.ListTopics(ctx, &pb.ListTopicsRequest{})
	if err != nil {
		return nil, errors.Wrap(err, "could not produce")
	}
	meta := r.GetMeta()
	if !meta.GetOK() {
		return nil, errors.Wrap(errors.New(meta.GetErrorMsg()), "broker error listing topics")
	}
	return r.GetTopics(), nil
}

func (c *Client) Produce(ctx context.Context, topic []byte, msgs ...[]byte) error {
	if len(msgs) == 0 {
		return nil
	}

	c.streamLock.Lock()
	defer c.streamLock.Unlock()

	// reconnect to stream if required
	err := c.streamConnect()
	if err != nil {
		return err
	}

	return c.produce(ctx, topic, msgs...)
}

func (c *Client) produce(ctx context.Context, topic []byte, msgs ...[]byte) error {
	msgSizes := make([]int64, len(msgs))
	var totalSize int64
	for i := range msgs {
		msgSizes[i] = int64(len(msgs[i]))
		totalSize += msgSizes[i]
	}
	if int64(len(c.streamBuf)) < totalSize {
		c.streamBuf = append(c.streamBuf, make([]byte, totalSize-int64(len(c.streamBuf)))...)
	}

	ctx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()

	// send message metadata
	r, err := c.client.Produce(ctx, &pb.ProduceRequest{
		Topic:    topic,
		Uuid:     c.id[:],
		MsgSizes: msgSizes,
	})
	if err != nil {
		return errors.Wrap(err, "could not produce")
	}
	meta := r.GetMeta()
	if !meta.GetOK() {
		return errors.Wrap(errors.New(meta.GetErrorMsg()), "broker error producing message")
	}

	// send messages
	var n int
	for i := range msgs {
		n += copy(c.streamBuf[n:], msgs[i])
	}
	_, err = c.stream.Write(c.streamBuf[:n])
	if err != nil {
		c.stream.Close()
		c.stream = nil
		return errors.Wrap(err, "unable to write stream")
	}
	var resp [2]byte
	_, err = io.ReadFull(c.stream, resp[:])
	if err != nil {
		c.stream.Close()
		c.stream = nil
		return errors.Wrap(err, "no stream response")
	}

	err = protocol.ResponseToError(resp)
	if err != nil {
		c.stream.Close()
		c.stream = nil
	}

	// create topic if required
	if c.config.CreateTopics && errors.Cause(err) == protocol.TopicDoesNotExist {
		err = c.CreateTopic(ctx, topic)
		if err == nil || errors.Cause(err) == protocol.TopicExistsErr {
			c.streamLock.Unlock()
			err = c.Produce(ctx, topic, msgs...)
			c.streamLock.Lock()
		}
	}
	return err
}

type ProduceMsg struct {
	Msg []byte
	Err chan error
}

func (c *Client) ProduceStream(ctx context.Context, topic []byte, ch chan ProduceMsg) error {
	if cap(ch) == 0 {
		return errors.New("invalid channel capacity, channels must have a capacity of at least 1")
	}
	c.streamLock.Lock()
	defer c.streamLock.Unlock()

	// reconnect to stream if required
	err := c.streamConnect()
	if err != nil {
		return err
	}

	errs := make([]chan error, 0, cap(ch))
	msgs := make([][]byte, 0, cap(ch))

	for msg := range ch {
		msgs = append(msgs, msg.Msg)
		if msg.Err != nil {
			errs = append(errs, msg.Err)
		}
		if len(msgs) == cap(ch) || (len(ch) == 0 && len(msgs) > 0) {
			// send produce batch
			err = c.produce(ctx, topic, msgs...)
			if err != nil {
				for i := range errs {
					errs[i] <- err
				}
				return err
			}
			for i := range errs {
				close(errs[i])
			}
			// truncate msg buffer
			msgs = msgs[:0]
			errs = errs[:0]
		}
	}

	if len(msgs) > 0 {
		//send one last batch
		err = c.produce(ctx, topic, msgs...)
		if err != nil {
			for i := range errs {
				errs[i] <- err
			}
			return err
		}
		for i := range errs {
			close(errs[i])
		}
	}
	return nil
}

func (c *Client) Consume(ctx context.Context, topic []byte, offset int64, maxBatchSize int64, resp *ConsumeResponse) error {
	if resp == nil {
		return errors.New("invalid ConsumeResponse")
	}

	ctx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()

	c.streamLock.Lock()
	defer c.streamLock.Unlock()

	// send message metadata
	r, err := c.client.Consume(ctx, &pb.ConsumeRequest{
		Topic:        topic,
		Uuid:         c.id[:],
		Offset:       offset,
		MaxBatchSize: maxBatchSize,
	})
	if err != nil {
		return errors.Wrap(err, "could not consume")
	}
	meta := r.GetMeta()
	if !meta.GetOK() {
		return errors.Wrap(errors.New(meta.GetErrorMsg()), "broker error consuming message")
	}

	resp.lock.Lock()
	resp.msgSizes = append(resp.msgSizes, r.GetMsgSizes()...)
	resp.conn = c.stream
	resp.lock.Unlock()

	return nil
}

type ConsumeResponse struct {
	lock     sync.Mutex
	msgSizes []int64
	conn     net.Conn
}

func (resp *ConsumeResponse) N() int {
	return len(resp.msgSizes)
}

func (resp *ConsumeResponse) BufSize() int64 {
	var n int64
	for i := range resp.msgSizes {
		n += resp.msgSizes[i]
	}
	return n
}

func sum(in []int64) int64 {
	var out int64
	for i := range in {
		out += in[i]
	}
	return out
}

func (resp *ConsumeResponse) Batch() ([][]byte, error) {
	resp.lock.Lock()
	defer resp.lock.Unlock()

	buf := make([]byte, sum(resp.msgSizes))
	_, err := io.ReadFull(resp.conn, buf)
	if err != nil {
		return nil, err
	}
	var n int
	output := make([][]byte, len(resp.msgSizes))
	for i := range output {
		output[i] = make([]byte, resp.msgSizes[i])
		n += copy(output[i], buf[n:])
	}
	resp.msgSizes = resp.msgSizes[:0]
	return output, nil
}

func (resp *ConsumeResponse) Next() ([]byte, error) {
	resp.lock.Lock()
	defer resp.lock.Unlock()
	if len(resp.msgSizes) == 0 {
		return nil, io.EOF
	}
	buf := make([]byte, resp.msgSizes[0])
	_, err := io.ReadFull(resp.conn, buf)
	if err != nil {
		return nil, err
	}
	copy(resp.msgSizes[0:], resp.msgSizes[1:])
	resp.msgSizes = resp.msgSizes[:len(resp.msgSizes)-1]

	return buf, nil
}
