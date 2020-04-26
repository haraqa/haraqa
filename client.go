//Package haraqa (High Availability Routing And Queueing Application) defines
// the go client for communicating with the haraqa broker:
// https://hub.docker.com/r/haraqa/haraqa .
//
package haraqa

import (
	"context"
	"io"
	"net"
	"regexp"
	"strconv"
	"time"

	"github.com/haraqa/haraqa/internal/protocol"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var (
	//ErrTopicExists is returned if a CreateTopic request is made to an existing topic
	ErrTopicExists = protocol.ErrTopicExists
	//ErrTopicDoesNotExist is returned if a request is made on a non existent topic
	ErrTopicDoesNotExist = protocol.ErrTopicDoesNotExist
)

// Client is the connection to the haraqa broker. While it's technically possible
// to produce and consume using the same client, it's recommended to use separate
// clients for producing and consuming. Use NewClient(config) to start a client
// session.
type Client struct {
	addr         string        // address of the haraqa broker
	gRPCPort     int           // broker's grpc port (default 4353)
	dataPort     int           // broker's data port (default 14353)
	unixSocket   string        // if set, the unix socket is used for the data connection
	createTopics bool          // if a topic does not exist, automatically create it
	timeout      time.Duration // the timeout for grpc requests, 0 for no timeout
	keepalive    time.Duration // the interval between ping messages to the data endpoint
	preProcess   []func(msgs [][]byte) error
	postProcess  []func(msgs [][]byte) error

	grpcConn   *grpc.ClientConn
	grpcClient protocol.HaraqaClient

	producerIn  chan func(net.Conn) error
	producerOut chan error
	consumerIn  chan func(net.Conn) error
	consumerOut chan error
	dataCloser  chan struct{}

	dataBuf []byte
}

// Defaults
const (
	DefaultAddr         = "127.0.0.1"
	DefaultGRPCPort     = 4353
	DefaultDataPort     = 14353
	DefaultUnixSocket   = ""
	DefaultCreateTopics = true
	DefaultTimeout      = time.Duration(0)
	DefaultKeepalive    = 120 * time.Second
)

// NewClient creates a new haraqa client based on the given config
//  client, err := haraqa.NewClient()
//  if err != nil {
//    panic(err)
//  }
//  defer client.Close()
func NewClient(options ...Option) (*Client, error) {
	c := &Client{
		addr:         DefaultAddr,
		gRPCPort:     DefaultGRPCPort,
		dataPort:     DefaultDataPort,
		unixSocket:   DefaultUnixSocket,
		createTopics: DefaultCreateTopics,
		timeout:      DefaultTimeout,
		keepalive:    DefaultKeepalive,
		preProcess:   make([]func([][]byte) error, 0),
		postProcess:  make([]func([][]byte) error, 0),
		producerIn:   make(chan func(net.Conn) error),
		producerOut:  make(chan error),
		consumerIn:   make(chan func(net.Conn) error),
		consumerOut:  make(chan error),
		dataCloser:   make(chan struct{}),
	}
	for _, opt := range options {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	// Set up a connection to the server.
	var err error
	ctx := context.Background()
	if c.timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	c.grpcConn, err = grpc.DialContext(ctx, c.addr+":"+strconv.Itoa(c.gRPCPort), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, errors.Wrapf(err, "unable to connect to grpc port %q", c.addr+":"+strconv.Itoa(c.gRPCPort))
	}
	c.grpcClient = protocol.NewHaraqaClient(c.grpcConn)

	// setup worker to process data requests
	data, err := c.dataConnect()
	if err != nil {
		return nil, errors.Wrapf(err, "unable to connect to data port %q", c.addr+":"+strconv.Itoa(c.dataPort))
	}
	go func() {
		var err, tmpErr error
		var tries int
		var f func(net.Conn) error
		var out chan error

		// best effort close connection
		defer protocol.Close(data)

		timer := time.NewTimer(c.keepalive)
		defer timer.Stop()
		for {
			select {
			case <-c.dataCloser:
				return
			case <-timer.C:
				err = protocol.Ping(data)
				if err != nil {
					// best effort close connection
					_ = data.Close()

					// attempt to reconnect
					data, _ = c.dataConnect()
				}
				timer.Reset(c.keepalive)
				continue

			case f = <-c.producerIn:
				out = c.producerOut
			case f = <-c.consumerIn:
				out = c.consumerOut
			}

			// max try twice
			for tries = 0; tries < 2; tries++ {
				err = f(data)
				// no error, moving on
				if err == nil {
					break
				}

				// attempt to reconnect
				data, tmpErr = c.dataConnect()
				if tmpErr != nil {
					out <- err
					break
				}

				// create topic error?
				if c.createTopics && out == c.producerOut && errors.Cause(err) == protocol.ErrTopicDoesNotExist {
					// retry
					continue
				}

				// network error?
				if _, ok := errors.Cause(err).(*net.OpError); ok {
					// retry
					continue
				}

				// won't retry
				break
			}
			out <- err
		}
	}()

	return c, nil
}

// Close closes the client connection
func (c *Client) Close() error {
	if c.dataCloser != nil {
		close(c.dataCloser)
	}

	if c.grpcConn != nil {
		err := c.grpcConn.Close()
		if err != nil {
			return errors.Wrap(err, "error closing haraqa client")
		}
	}

	return nil
}

// dataConnect connects a new data client connection to the haraqa broker. it should be called
// before any consume or produce grpc calls
func (c *Client) dataConnect() (net.Conn, error) {
	var err error
	var dataConn net.Conn
	// connect to data port
	if c.unixSocket != "" {
		dataConn, err = net.Dial("unix", c.unixSocket)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to connect to unix socket %q", c.unixSocket)
		}
	} else {
		dataConn, err = net.Dial("tcp", c.addr+":"+strconv.Itoa(c.dataPort))
		if err != nil {
			return nil, errors.Wrapf(err, "unable to connect to data port %q", c.addr+":"+strconv.Itoa(c.dataPort))
		}
	}

	return dataConn, nil
}

//CreateTopic creates a new topic. It returns a ErrTopicExists error if the
// topic has already been created
func (c *Client) CreateTopic(ctx context.Context, topic []byte) error {
	if c.timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	// send message request
	r, err := c.grpcClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic: topic,
	})
	if err != nil {
		return errors.Wrap(err, "could not produce")
	}
	meta := r.GetMeta()
	if !meta.GetOK() {
		switch meta.GetErrorMsg() {
		case protocol.ErrTopicExists.Error():
			return ErrTopicExists
		default:
		}
		return errors.Wrapf(errors.New(meta.GetErrorMsg()), "broker error creating topic %q", string(topic))
	}
	return nil
}

// DeleteTopic permanentaly deletes all messages in a topic queue
func (c *Client) DeleteTopic(ctx context.Context, topic []byte) error {
	if c.timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	// send message request
	r, err := c.grpcClient.DeleteTopic(ctx, &protocol.DeleteTopicRequest{
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

// ListTopics queries the broker for a list of topics.
// If prefix is given, only topics matching the prefix are included.
// If suffix is given, only topics matching the suffix are included.
// If regex is given, only topics matching the regexp are included.
func (c *Client) ListTopics(ctx context.Context, prefix, suffix, regex string) ([][]byte, error) {
	// check regex before attempting
	if regex != "" {
		_, err := regexp.Compile(regex)
		if err != nil {
			return nil, err
		}
	}

	if c.timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	// send message request
	r, err := c.grpcClient.ListTopics(ctx, &protocol.ListTopicsRequest{Prefix: prefix, Suffix: suffix, Regex: regex})
	if err != nil {
		return nil, errors.Wrap(err, "could not produce")
	}
	meta := r.GetMeta()
	if !meta.GetOK() {
		return nil, errors.Wrap(errors.New(meta.GetErrorMsg()), "broker error listing topics")
	}
	return r.GetTopics(), nil
}

// Produce sends the message(s) to the broker topic as a single batch. If
//  config.CreateTopic is true it will automatically create the topic if it
//  doesn't already exist. Otherwise, if the topic does not exist Produce will
//  return error haraqa.ErrTopicDoesNotExist
func (c *Client) Produce(ctx context.Context, topic []byte, msgs ...[]byte) error {
	if len(msgs) == 0 {
		return nil
	}

	for _, process := range c.preProcess {
		if err := process(msgs); err != nil {
			return err
		}
	}

	c.producerIn <- func(conn net.Conn) error {
		return c.produce(ctx, conn, topic, msgs...)
	}
	return <-c.producerOut
}

func (c *Client) produce(ctx context.Context, conn net.Conn, topic []byte, msgs ...[]byte) error {
	var err error
	msgSizes := make([]int64, len(msgs))
	var totalSize int64
	for i := range msgs {
		msgSizes[i] = int64(len(msgs[i]))
		totalSize += msgSizes[i]
	}

	protocol.ExtendBuffer(&c.dataBuf, int(totalSize))
	req := protocol.ProduceRequest{
		Topic:    topic,
		MsgSizes: msgSizes,
	}
	err = req.Write(conn)
	if err != nil {
		return errors.Wrap(err, "could not write produce header")
	}

	// send messages
	var n int
	for i := range msgs {
		n += copy(c.dataBuf[n:], msgs[i])
	}
	_, err = conn.Write(c.dataBuf[:n])
	if err != nil {
		return errors.Wrap(err, "unable to write messages to data connection")
	}

	var prefix [6]byte
	var p byte
	p, _, err = protocol.ReadPrefix(conn, prefix[:])
	if err != nil {
		if c.createTopics && errors.Cause(err) == protocol.ErrTopicDoesNotExist {
			e := c.CreateTopic(ctx, topic)
			if e != nil && errors.Cause(e) != protocol.ErrTopicExists {
				return e
			}
		}
		return errors.Wrap(err, "could not read from data connection")
	}
	if p != protocol.TypeProduce {
		return errors.New("invalid response read from data connection")
	}

	return nil
}

// Offsets returns the min and max offsets available for a topic
//  min, max, err := client.Offset([]byte("myTopic"))
func (c *Client) Offsets(ctx context.Context, topic []byte) (int64, int64, error) {
	if c.timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	resp, err := c.grpcClient.Offsets(ctx, &protocol.OffsetRequest{
		Topic: topic,
	})
	if err != nil {
		return 0, 0, err
	}
	if !resp.GetMeta().GetOK() {
		if resp.GetMeta().GetErrorMsg() == protocol.ErrTopicDoesNotExist.Error() {
			return 0, 0, protocol.ErrTopicDoesNotExist
		}
		return 0, 0, errors.New(resp.GetMeta().GetErrorMsg())
	}

	return resp.GetMinOffset(), resp.GetMaxOffset(), nil
}

// ConsumeBuffer is a reusable set of buffers used to consume. The use of a
// ConsumeBuffer prevents unnecessary allocations when consuming.
//  buf := NewConsumerBuffer()
//  for {
//		msgs, err := c.Consume(ctx, topic, offset, limit, buf)
//		if err != nil {
//			panic(err)
//		}
//		for _, msg := range msgs{
//			fmt.Println(string(msg))
//		}
//		offset += int64(len(msgs))
//  }
type ConsumeBuffer struct {
	headerBuf []byte
	bodyBuf   []byte
	msgSizes  []int64
	msgBuf    [][]byte
}

// NewConsumeBuffer instantiates a new ConsumeBuffer
func NewConsumeBuffer() *ConsumeBuffer {
	return new(ConsumeBuffer)
}

// Consume sends a consume request and returns a batch of messages, buf can be nil.
//  If the topic does not exist Consume returns haraqa.ErrTopicDoesNotExist.
//  If offset is less than 0, the maximum offset of the topic is used.
//  If limit is less than 0, the broker determines the number of messages sent
func (c *Client) Consume(ctx context.Context, topic []byte, offset int64, limit int64, buf *ConsumeBuffer) ([][]byte, error) {
	if buf == nil {
		buf = NewConsumeBuffer()
	}

	var msgs [][]byte
	c.consumerIn <- func(conn net.Conn) error {
		var err error
		msgs, err = c.consume(ctx, conn, topic, offset, limit, buf)
		return err
	}
	err := <-c.consumerOut
	return msgs, err
}

func (c *Client) consume(ctx context.Context, conn net.Conn, topic []byte, offset int64, limit int64, buf *ConsumeBuffer) ([][]byte, error) {
	req := protocol.ConsumeRequest{
		Topic:  topic,
		Offset: offset,
		Limit:  limit,
	}

	err := req.Write(conn)
	if err != nil {
		return nil, errors.Wrap(err, "could not write to data connection")
	}

	var (
		p      byte
		hLen   uint32
		prefix [6]byte
	)
	p, hLen, err = protocol.ReadPrefix(conn, prefix[:])
	if err != nil {
		if errors.Cause(err) == protocol.ErrTopicDoesNotExist {
			return nil, ErrTopicDoesNotExist
		}
		return nil, errors.Wrapf(err, "could not read from data connection")
	}
	if p != protocol.TypeConsume {
		return nil, errors.New("invalid response type read from data connection")
	}

	protocol.ExtendBuffer(&buf.headerBuf, int(hLen))
	_, err = io.ReadFull(conn, buf.headerBuf)
	if err != nil {
		return nil, errors.Wrap(err, "could not read error from data connection")
	}

	resp := protocol.ConsumeResponse{
		MsgSizes: buf.msgSizes,
	}
	err = resp.Read(buf.headerBuf)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid response read from data connection")
	}
	if cap(resp.MsgSizes) > cap(buf.msgSizes) {
		buf.msgSizes = resp.MsgSizes
	}

	totalSize := sum(resp.MsgSizes)
	protocol.ExtendBuffer(&buf.bodyBuf, int(totalSize))

	_, err = io.ReadFull(conn, buf.bodyBuf)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read batch messages from connection")
	}

	if len(resp.MsgSizes) > cap(buf.msgBuf) {
		buf.msgBuf = append(buf.msgBuf, make([][]byte, len(resp.MsgSizes)-len(buf.msgBuf))...)
	}
	buf.msgBuf = (buf.msgBuf)[:len(resp.MsgSizes)]

	var n int64
	for i := range resp.MsgSizes {
		buf.msgBuf[i] = buf.bodyBuf[n : n+resp.MsgSizes[i] : n+resp.MsgSizes[i]]
		n += resp.MsgSizes[i]
	}
	for _, process := range c.postProcess {
		if e := process(buf.msgBuf); e != nil {
			return nil, e
		}
	}
	return buf.msgBuf, nil
}

func sum(in []int64) int64 {
	var out int64
	for i := range in {
		out += in[i]
	}
	return out
}

// contextErrorOverride sends the ctx.Error() if present, otherwise it sends err
func contextErrorOverride(ctx context.Context, err error) error {
	// check if error was cause by context deadline
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// Lock sends a lock request to the broker to implement a distributed lock.
//  If 'blocking' is set to true it will block until a lock has been acquired
func (c *Client) Lock(ctx context.Context, groupName []byte, blocking bool) (io.Closer, bool, error) {
	l, err := c.grpcClient.Lock(ctx)
	if err != nil {
		return nil, false, err
	}
	req := &protocol.LockRequest{
		Group: groupName,
		Time:  c.timeout.Milliseconds(),
		Lock:  true,
	}

	if !blocking {
		req.Time = 0
	}

	for {
		select {
		case <-ctx.Done():
		default:
		}
		err = l.Send(req)
		if err != nil {
			return nil, false, contextErrorOverride(ctx, err)
		}
		resp, err := l.Recv()
		if err != nil {
			return nil, false, contextErrorOverride(ctx, err)
		}
		if resp.GetLocked() {
			break
		} else if !blocking {
			return nil, false, nil
		}
	}

	return &lockCloser{
		l:         l,
		groupName: groupName,
	}, true, nil
}

type lockCloser struct {
	l         protocol.Haraqa_LockClient
	groupName []byte
}

func (c *lockCloser) Close() error {
	return c.l.Send(&protocol.LockRequest{
		Group: c.groupName,
		Lock:  false,
	})
}
