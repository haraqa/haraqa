//Package haraqa (High Availability Routing And Queueing Application) defines
// the go client for communicating with the haraqa broker:
// https://hub.docker.com/repository/docker/haraqa/haraqa .
//
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

//DefaultConfig is the configuration for standard, local deployment of the haraqa broker
var DefaultConfig = Config{
	Host:         "127.0.0.1",
	GRPCPort:     4353,
	DataPort:     14353,
	CreateTopics: true,
	Timeout:      time.Second * 5,
}

//Config for new clients, see DefaultConfig for recommended values
type Config struct {
	Host         string        // address of the haraqa broker
	GRPCPort     int           // broker's grpc port (default 4353)
	DataPort     int           // broker's data port (default 14353)
	CreateTopics bool          // if a topic does not exist, automatically create it
	UnixSocket   string        // if set, the unix socket is used for the data connection
	Timeout      time.Duration // the timeout for grpc requests
}

// Client is the connection to the haraqa broker. While it's technically possible
// to produce and consume using the same client, it's recommended to use separate
// clients for producing and consuming. Use NewClient(config) to start a client
// session.
type Client struct {
	config       Config
	id           [16]byte
	grpcConn     *grpc.ClientConn
	client       pb.HaraqaClient
	dataConnLock sync.Mutex
	dataConn     net.Conn
	dataBuf      []byte
}

// NewClient creates a new haraqa client based on the given config
//  cfg := haraqa.DefaultConfig
//  client, err := haraqa.NewClient(cfg)
//  if err != nil {
//    panic(err)
//  }
//  defer client.Close()
func NewClient(config Config) (*Client, error) {
	if config.Host == "" {
		return nil, errors.New("invalid host")
	}
	if config.GRPCPort == 0 || (config.UnixSocket == "" && config.DataPort == 0) {
		return nil, errors.New("invalid ports")
	}

	// Set up a connection to the server.
	grpcConn, err := grpc.Dial(config.Host+":"+strconv.Itoa(config.GRPCPort), grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(config.Timeout))
	if err != nil {
		return nil, errors.Wrapf(err, "unable to connect to grpc port %q", config.Host+":"+strconv.Itoa(config.GRPCPort))
	}
	client := pb.NewHaraqaClient(grpcConn)
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
		config:   config,
		grpcConn: grpcConn,
		client:   client,
		id:       id,
	}

	return c, nil
}

// Close closes the client connection
func (c *Client) Close() error {
	c.dataConnLock.Lock()
	defer c.dataConnLock.Unlock()

	var errs []error
	ctx, cancel := context.WithTimeout(context.Background(), c.config.Timeout)
	defer cancel()
	if _, err := c.client.CloseConnection(ctx, &protocol.CloseRequest{Uuid: c.id[:]}); err != nil {
		errs = append(errs, err)
	}

	if err := c.grpcConn.Close(); err != nil {
		errs = append(errs, err)
	}
	if c.dataConn != nil {
		if err := c.dataConn.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Wrapf(errs[0], "error closing haraqa client total errors: (%d)", len(errs))
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
	if c.config.UnixSocket != "" {
		dataConn, err = net.Dial("unix", c.config.UnixSocket)
		if err != nil {
			return errors.Wrapf(err, "unable to connect to unix socket %q", c.config.UnixSocket)
		}
	} else {
		dataConn, err = net.Dial("tcp", c.config.Host+":"+strconv.Itoa(c.config.DataPort))
		if err != nil {
			return errors.Wrapf(err, "unable to connect to data port %q", c.config.Host+":"+strconv.Itoa(c.config.DataPort))
		}
	}

	// initialize dataConn with id
	_, err = dataConn.Write(c.id[:])
	if err != nil {
		return errors.Wrap(err, "unable write to data port")
	}

	var resp [2]byte
	_, err = io.ReadFull(dataConn, resp[:])
	if err != nil {
		return errors.Wrap(err, "unable read from data port")
	}
	c.dataConn = dataConn
	return nil
}

//CreateTopic creates a new topic. It returns a ErrTopicExists error if the
// topic has already been created
func (c *Client) CreateTopic(ctx context.Context, topic []byte) error {
	ctx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()

	// send message request
	r, err := c.client.CreateTopic(ctx, &pb.CreateTopicRequest{
		Topic: topic,
	})
	if err != nil {
		return errors.Wrap(err, "could not produce")
	}
	meta := r.GetMeta()
	if !meta.GetOK() {
		switch meta.GetErrorMsg() {
		case protocol.ErrTopicExists.Error():
			err = protocol.ErrTopicExists
		default:
			err = errors.New(meta.GetErrorMsg())
		}
		return errors.Wrapf(err, "broker error creating topic %q", string(topic))
	}
	return nil
}

// DeleteTopic permanentaly deletes all messages in a topic queue
func (c *Client) DeleteTopic(ctx context.Context, topic []byte) error {
	ctx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()

	// send message request
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

// ListTopics queries the broker for a list of topics
func (c *Client) ListTopics(ctx context.Context) ([][]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()

	// send message request
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

// Produce one or more messages as a batch to a common topic
func (c *Client) Produce(ctx context.Context, topic []byte, msgs ...[]byte) error {
	if len(msgs) == 0 {
		return nil
	}

	// reconnect to data endpoint if required
	err := c.dataConnect()
	if err != nil {
		return err
	}

	return c.produce(ctx, topic, msgs...)
}

func (c *Client) produce(ctx context.Context, topic []byte, msgs ...[]byte) error {
	c.dataConnLock.Lock()
	defer c.dataConnLock.Unlock()

	msgSizes := make([]int64, len(msgs))
	var totalSize int64
	for i := range msgs {
		msgSizes[i] = int64(len(msgs[i]))
		totalSize += msgSizes[i]
	}
	if int64(len(c.dataBuf)) < totalSize {
		c.dataBuf = append(c.dataBuf, make([]byte, totalSize-int64(len(c.dataBuf)))...)
	}

	ctx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()

	// send message request
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
		n += copy(c.dataBuf[n:], msgs[i])
	}
	_, err = c.dataConn.Write(c.dataBuf[:n])
	if err != nil {
		c.dataConn.Close()
		c.dataConn = nil
		return errors.Wrap(err, "unable to write data connection")
	}
	var resp [2]byte
	_, err = io.ReadFull(c.dataConn, resp[:])
	if err != nil {
		c.dataConn.Close()
		c.dataConn = nil
		return errors.Wrap(err, "no data connection response")
	}

	err = protocol.ResponseToError(resp)
	if err != nil {
		c.dataConn.Close()
		c.dataConn = nil
	}

	// create topic if required
	if c.config.CreateTopics && errors.Cause(err) == protocol.ErrTopicDoesNotExist {
		err = c.CreateTopic(ctx, topic)
		if err == nil || errors.Cause(err) == protocol.ErrTopicExists {
			c.dataConnLock.Unlock()
			err = c.Produce(ctx, topic, msgs...)
			c.dataConnLock.Lock()
		}
	}
	return err
}

// ProduceMsg is the message structure for sending messages to a ProduceLoop channel.
// The Err channel must be set with a capacity of 1 or greater to receive an error response.
// if the message was produced successfully a nil error is returned.
type ProduceMsg struct {
	Msg []byte
	Err chan error
}

// NewProduceMsg returns a ProduceMsg with a new Err channel. Use with the ProduceLoop method.
func NewProduceMsg(msg []byte) ProduceMsg {
	return ProduceMsg{
		Msg: msg,
		Err: make(chan error, 1),
	}
}

// ProduceLoop starts a loop that reads from the channel and sends the messages as
// a batch to the broker. The batch size, the number of messages in a batch, is equal
// to the capacity of the channel given.
// If the capacity is 0 an error is returned. Batches do not need to be filled before being
// sent so it is recommended to set the batch size to a reasonably high value.
// Messages are sent when either the number of messages reaches the channel capacity
// or when the channel has been drained and there are no remaining messages in the channel.
// If the channel is closed the ProduceLoop is gracefully stopped and any remaining
// messages in the channel are sent
func (c *Client) ProduceLoop(ctx context.Context, topic []byte, ch chan ProduceMsg) error {
	if cap(ch) == 0 {
		return errors.New("invalid channel capacity, channels must have a capacity of at least 1")
	}

	// reconnect to data endpoint if required
	if err := c.dataConnect(); err != nil {
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
			err := c.produce(ctx, topic, msgs...)
			for i := range errs {
				errs[i] <- err
			}
			if err != nil {
				return err
			}
			// truncate msg buffer
			msgs = msgs[:0]
			errs = errs[:0]
		}
	}

	if len(msgs) > 0 {
		//send one last batch
		err := c.produce(ctx, topic, msgs...)
		for i := range errs {
			errs[i] <- err
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Offsets returns the min and max offsets available for a topic
//  min, max, err := client.Offset([]byte("myTopic"))
func (c *Client) Offsets(ctx context.Context, topic []byte) (int64, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()

	resp, err := c.client.Offsets(ctx, &protocol.OffsetRequest{
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

// Consume starts a consume request and adds messages to the ConsumeResponse.
// Consume is not thread safe and Consume or Produce messages should not be called
// until resp.N() is zero or Batch() is called. Offest is the number of messages to skip
// when consuming. If offset < 0 the last available offset is used
// maxBatchSize is the maximum number of messages to consume at a single time.
func (c *Client) Consume(ctx context.Context, topic []byte, offset int64, maxBatchSize int64, resp *ConsumeResponse) error {
	if resp == nil {
		return errors.New("invalid ConsumeResponse")
	}

	ctx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()

	c.dataConnLock.Lock()
	defer c.dataConnLock.Unlock()

	err := c.dataConnect()
	if err != nil {
		return errors.Wrap(err, "could not connect")
	}

	// send message request
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
	resp.dataConn = c.dataConn
	resp.lock.Unlock()

	return nil
}

// ConsumeResponse is used by the Client Consume method. It reads data from the client
// connection using either the Batch or Next methods.
type ConsumeResponse struct {
	lock     sync.Mutex
	msgSizes []int64
	dataConn net.Conn
}

// N is the number of messages remaining in the response. Use with the Next() method.
//  for resp.N() > 0 {
//    msg, err := resp.Next()
//    if err != nil {
//      panic(err)
//    }
//    handle(msg)
//  }
func (resp *ConsumeResponse) N() int {
	return len(resp.msgSizes)
}

func sum(in []int64) int64 {
	var out int64
	for i := range in {
		out += in[i]
	}
	return out
}

// Batch returns all of the consume response messages at once
func (resp *ConsumeResponse) Batch() ([][]byte, error) {
	resp.lock.Lock()
	defer resp.lock.Unlock()

	buf := make([]byte, sum(resp.msgSizes))
	_, err := io.ReadFull(resp.dataConn, buf)
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

// WriteTo writes all of the consume response messages at once to an io.Writer
func (resp *ConsumeResponse) WriteTo(w io.Writer) (int64, error) {
	resp.lock.Lock()
	defer resp.lock.Unlock()

	return io.CopyN(w, resp.dataConn, sum(resp.msgSizes))
}

// Next returns the next message from the queue. When all the messages in the batch
// have been read it returns an io.EOF error.
func (resp *ConsumeResponse) Next() ([]byte, error) {
	resp.lock.Lock()
	defer resp.lock.Unlock()
	if len(resp.msgSizes) == 0 {
		return nil, io.EOF
	}
	buf := make([]byte, resp.msgSizes[0])
	_, err := io.ReadFull(resp.dataConn, buf)
	if err != nil {
		return nil, err
	}
	copy(resp.msgSizes[0:], resp.msgSizes[1:])
	resp.msgSizes = resp.msgSizes[:len(resp.msgSizes)-1]

	return buf, nil
}
