package haraqa

import (
	"context"
	"sync"

	"github.com/pkg/errors"
)

// ProducerOption is used with (*Client).NewProducer to set producer parameters and override defaults
type ProducerOption func(*Producer) error

// WithBatchSize sets the maximum batch size a Producer will use. Batch size is the number
// of messages sent in an individual request
func WithBatchSize(size int) ProducerOption {
	return func(p *Producer) error {
		if size <= 0 {
			return errors.New("batch size must be greater than 0")
		}
		p.batchSize = size
		return nil
	}
}

// WithIgnoreErrors sets the (*Producer).Send method to not wait for a response
// from the broker. If given, WithIgnoreErrors will cause (*Producer).Send to always
// return nil, until the producer is closed.
// IgnoreErrors does not effect WithErrorHandler.
func WithIgnoreErrors() ProducerOption {
	return func(p *Producer) error {
		p.ignoreErrs = true
		return nil
	}
}

// WithTopic defines the topic a NewProducer should produce to. This is required and
// cannot be changed for a producer
func WithTopic(topic []byte) ProducerOption {
	return func(p *Producer) error {
		p.topic = topic
		return nil
	}
}

// WithContext allows a context.Context to be passed to the produce function. This is
// only used if the CreateTopic method is called and a parent process cancels the ctx.
func WithContext(ctx context.Context) ProducerOption {
	return func(p *Producer) error {
		p.ctx = ctx
		return nil
	}
}

// WithErrorHandler allows custom logic to be added to a Producer to act if an error occurs
// during a Send operation. The error is the same error returned by (*Producer).Send
func WithErrorHandler(handler func(msgs [][]byte, err error)) ProducerOption {
	return func(p *Producer) error {
		if handler == nil {
			return errors.New("invalid error handler")
		}
		p.errHandler = handler
		return nil
	}
}

// Producer provides an efficient way to send multiple messages to a single topic
// in a concurrent, safe way.
//  // setup client, see haraqa.NewClient() for more details
//  client, err := haraqa.NewClient
//  if err != nil {
//    panic(err)
//  }
//  defer client.Close()
//
//  // setup new producer
//  topic := []byte("my_topic")
//  producer, err := client.NewProducer(haraqa.WithTopic(topic))
//  if err != nil {
//    panic(err)
//  }
//  defer producer.Close()
//
//  // send messages (it's safe to send from multiple goroutines)
//  producer.Send([]byte("some message"))
type Producer struct {
	batchSize  int
	ignoreErrs bool
	topic      []byte
	c          *Client
	ctx        context.Context
	errHandler func([][]byte, error)
	msgs       chan produceMsg // used by Send
	rawMsgs    chan []byte     // used by Send (when ignoreErrs true)
	errs       chan chan error // used by getErrs/putErrs in Send
	wg         sync.WaitGroup  // used when closing producer
	closed     bool            // used when closing producer
}

type produceMsg struct {
	Msg []byte
	Err chan error
}

func noopErrHandler([][]byte, error) {}

// NewProducer instantiates a new Producer type
func (c *Client) NewProducer(opts ...ProducerOption) (*Producer, error) {
	p := &Producer{
		batchSize:  1024,
		ignoreErrs: false,
		c:          c,
		ctx:        context.Background(),
		errHandler: noopErrHandler,
	}
	for _, opt := range opts {
		err := opt(p)
		if err != nil {
			return nil, errors.Wrap(err, "invalid option")
		}
	}
	if len(p.topic) == 0 {
		err := errors.New("missing topic")
		return nil, errors.Wrap(err, "invalid option")
	}

	// handle simple case, ignoring errors
	if p.ignoreErrs {
		p.rawMsgs = make(chan []byte, p.batchSize)
		go func() {
			msgs := make([][]byte, 0, p.batchSize)
			for {
				msg, ok := <-p.rawMsgs
				if !ok {
					return
				}
				msgs = append(msgs, msg)
				if len(msgs) == p.batchSize || (len(p.rawMsgs) == 0 && len(msgs) > 0) {
					_ = p.process(msgs)

					// truncate msg buffer
					msgs = msgs[:0]
				}
			}
		}()
		return p, nil
	}

	// init channels
	p.msgs = make(chan produceMsg, p.batchSize)
	p.errs = make(chan chan error, 2*p.batchSize)
	// init some error channels in the pool
	for i := 0; i < p.batchSize*2; i++ {
		p.putErrs(make(chan error, 1))
	}

	// handle more complex case, return err to send
	go func() {
		errs := make([]chan error, 0, p.batchSize)
		msgs := make([][]byte, 0, p.batchSize)
		var ok bool

		for {
			var msg produceMsg
			msg, ok = <-p.msgs
			if !ok {
				return
			}
			msgs = append(msgs, msg.Msg)
			errs = append(errs, msg.Err)
			if len(msgs) == p.batchSize || (len(p.msgs) == 0 && len(msgs) > 0) {
				err := p.process(msgs)
				for i := range errs {
					errs[i] <- err
				}

				// truncate msg buffer
				msgs = msgs[:0]
				errs = errs[:0]
			}
		}
	}()

	return p, nil
}

// Close stops the Producer background process. Calling Send after Close will result
// in Send receiving an error
func (p *Producer) Close() error {
	p.closed = true
	p.wg.Wait()
	if p.msgs != nil {
		close(p.msgs)
	}
	if p.rawMsgs != nil {
		close(p.rawMsgs)
	}
	return nil
}

func (p *Producer) process(msgs [][]byte) error {
	//send batch
	p.c.dataConnLock.Lock()
	err := p.c.produce(p.ctx, p.topic, msgs...)
	p.c.dataConnLock.Unlock()

	if err != nil {
		p.errHandler(msgs, err)
	}
	return err
}

// Send produces a message to the haraqa broker
func (p *Producer) Send(msg []byte) error {
	// preprocess
	for _, process := range p.c.preProcess {
		if err := process([][]byte{msg}); err != nil {
			return err
		}
	}
	p.wg.Add(1)
	defer p.wg.Done()
	if p.closed {
		return errors.New("cannot send on closed producer")
	}
	if p.ignoreErrs {
		p.rawMsgs <- msg
		return nil
	}

	errs := p.getErrs()
	defer p.putErrs(errs)
	p.msgs <- produceMsg{
		Msg: msg,
		Err: errs,
	}
	return <-errs
}

func (p *Producer) getErrs() chan error {
	return <-p.errs
}

func (p *Producer) putErrs(errs chan error) {
	// empty channel if not already empty
	select {
	case <-errs:
	default:
	}
	p.errs <- errs
}
