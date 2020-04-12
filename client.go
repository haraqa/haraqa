//Package haraqa (High Availability Routing And Queueing Application) defines
// the go client for communicating with the haraqa broker:
// https://hub.docker.com/repository/docker/haraqa/haraqa .
//
package haraqa

import (
	"context"
	"io"
	"net"
	"regexp"
	"strconv"
	"sync"
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
	preProcess   []func(msgs [][]byte) error
	postProcess  []func(msgs [][]byte) error

	grpcConn     *grpc.ClientConn
	grpcClient   protocol.HaraqaClient
	dataConnLock sync.Mutex
	dataConn     net.Conn
	dataBuf      []byte
}

// NewClient creates a new haraqa client based on the given config
//  client, err := haraqa.NewClient()
//  if err != nil {
//    panic(err)
//  }
//  defer client.Close()
func NewClient(options ...Option) (*Client, error) {

	c := &Client{
		addr:         "127.0.0.1",
		gRPCPort:     4353,
		dataPort:     14353,
		unixSocket:   "",
		createTopics: true,
		timeout:      0,
		preProcess:   make([]func([][]byte) error, 0),
		postProcess:  make([]func([][]byte) error, 0),
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

	return c, nil
}

// Close closes the client connection
func (c *Client) Close() error {
	c.dataConnLock.Lock()
	defer c.dataConnLock.Unlock()

	errs := make([]error, 0, 2)
	if c.grpcConn != nil {
		errs = append(errs, c.grpcConn.Close())
	}
	if c.dataConn != nil {
		errs = append(errs, c.dataConn.Close())
	}

	for i := range errs {
		if errs[i] != nil {
			return errors.Wrap(errs[i], "error closing haraqa client")
		}
	}
	return nil
}

// dataConnect connects a new data client connection to the haraqa broker. it should be called
// before any consume or produce grpc calls
func (c *Client) dataConnect() error {
	if c.dataConn != nil {
		return nil
	}
	var err error
	var dataConn net.Conn
	// connect to data port
	if c.unixSocket != "" {
		dataConn, err = net.Dial("unix", c.unixSocket)
		if err != nil {
			return errors.Wrapf(err, "unable to connect to unix socket %q", c.unixSocket)
		}
	} else {
		dataConn, err = net.Dial("tcp", c.addr+":"+strconv.Itoa(c.dataPort))
		if err != nil {
			return errors.Wrapf(err, "unable to connect to data port %q", c.addr+":"+strconv.Itoa(c.dataPort))
		}
	}

	c.dataConn = dataConn
	return nil
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

	c.dataConnLock.Lock()
	defer c.dataConnLock.Unlock()

	return c.produce(ctx, topic, msgs...)
}

func (c *Client) produce(ctx context.Context, topic []byte, msgs ...[]byte) error {
	// reconnect to data endpoint if required
	err := c.dataConnect()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			c.dataConn.Close()
			c.dataConn = nil
		}
	}()

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
	err = req.Write(c.dataConn)
	if err != nil {
		// check for broken pipe, try to reconnect and retry
		c.dataConn = nil
		if c.dataConnect() == nil {
			err = req.Write(c.dataConn)
		}
		if err != nil {
			return errors.Wrap(err, "could not write produce header")
		}
	}

	// send messages
	var n int
	for i := range msgs {
		n += copy(c.dataBuf[n:], msgs[i])
	}
	_, err = c.dataConn.Write(c.dataBuf[:n])
	if err != nil {
		return errors.Wrap(err, "unable to write data connection")
	}

	var prefix [6]byte
	var p byte
	p, _, err = protocol.ReadPrefix(c.dataConn, prefix[:])
	if err != nil {
		if errors.Cause(err) == protocol.ErrTopicDoesNotExist {
			err = c.CreateTopic(ctx, topic)
			if err != nil && errors.Cause(err) != protocol.ErrTopicExists {
				return err
			}
			return c.produce(ctx, topic, msgs...)
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
	c.dataConnLock.Lock()
	defer c.dataConnLock.Unlock()

	err := c.dataConnect()
	if err != nil {
		return nil, errors.Wrap(err, "could not connect to data port")
	}
	defer func() {
		if err != nil {
			c.dataConn.Close()
			c.dataConn = nil
		}
	}()

	req := protocol.ConsumeRequest{
		Topic:  topic,
		Offset: offset,
		Limit:  limit,
	}

	err = req.Write(c.dataConn)
	if err != nil {
		// check for broken pipe, try to reconnect and retry
		c.dataConn = nil
		if c.dataConnect() == nil {
			err = req.Write(c.dataConn)
		}
		if err != nil {
			return nil, errors.Wrap(err, "could not write to data connection")
		}
	}

	var (
		p      byte
		hLen   uint32
		prefix [6]byte
	)
	p, hLen, err = protocol.ReadPrefix(c.dataConn, prefix[:])
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
	_, err = io.ReadFull(c.dataConn, buf.headerBuf)
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

	_, err = io.ReadFull(c.dataConn, buf.bodyBuf)
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

// WatchEvent is the structure returned by the WatchTopics channel. It
// represents the min and max offsets of a topic when that topic receives new messages.
type WatchEvent struct {
	Topic     []byte
	MinOffset int64
	MaxOffset int64
}

// WatchTopics sets up a loop to watch for new offsets in a topic, responses are
//  sent at haraqa.WatchEvent structs. The loop blocks until an error is found
//  or if there is a context cancel event
func (c *Client) WatchTopics(ctx context.Context, ch chan WatchEvent, topics ...[]byte) error {
	if len(topics) == 0 {
		return errors.New("invalid number of topics sent")
	}
	stream, err := c.grpcClient.WatchTopics(ctx)
	if err != nil {
		return contextErrorOverride(ctx, err)
	}

	req := protocol.WatchRequest{
		Topics: topics,
	}
	err = stream.Send(&req)
	if err != nil {
		return contextErrorOverride(ctx, err)
	}

	// receive ack
	resp, err := stream.Recv()
	if err != nil {
		return contextErrorOverride(ctx, err)
	}
	if !resp.GetMeta().GetOK() {
		return errors.New(resp.GetMeta().GetErrorMsg())
	}

	defer func() {
		// best effort send close request
		_ = stream.Send(&protocol.WatchRequest{
			Term: true,
		})
		_ = stream.CloseSend()
	}()

	for {
		// check for a cancel event
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// wait until the stream sends a message
		resp, err := stream.Recv()
		if err != nil {
			return contextErrorOverride(ctx, err)
		}
		if !resp.GetMeta().GetOK() {
			return errors.New(resp.GetMeta().GetErrorMsg())
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case ch <- WatchEvent{
			Topic:     resp.GetTopic(),
			MinOffset: resp.GetMinOffset(),
			MaxOffset: resp.GetMaxOffset(),
		}:
		}
	}
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
