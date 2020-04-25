package haraqa

import (
	"context"

	"github.com/haraqa/haraqa/internal/protocol"
	"github.com/pkg/errors"
)

// WatchEvent is the structure returned by the WatchTopics channel. It
// represents the min and max offsets of a topic when that topic receives new messages.
// See Watcher for more details
type WatchEvent struct {
	Topic     []byte
	MinOffset int64
	MaxOffset int64
}

// Watcher listens on one or more topics for notifications of new messages
//  w, err := client.NewWatcher(ctx, []byte{"topic"})
//  if err != nil {
//    panic(err)
//  }
//  // process new events
//  for event := range w.Events(){
//    fmt.Println(event.Topic, event.MinOffset, event.MaxOffset)
//  }
//  // close and check error
//  err = w.Close()
//  if err != nil {
//    panic(err)
//  }
type Watcher struct {
	ch     chan WatchEvent
	errs   chan error
	done   chan struct{}
	stream protocol.Haraqa_WatchTopicsClient
}

// NewWatcher creates a new Watcher
func (c *Client) NewWatcher(ctx context.Context, topics ...[]byte) (*Watcher, error) {
	if len(topics) == 0 {
		return nil, errors.New("missing topics from NewWatcher request")
	}

	stream, err := c.grpcClient.WatchTopics(ctx)
	if err != nil {
		return nil, contextErrorOverride(ctx, err)
	}

	req := protocol.WatchRequest{
		Topics: topics,
	}
	err = stream.Send(&req)
	if err != nil {
		return nil, contextErrorOverride(ctx, err)
	}

	// receive ack
	resp, err := stream.Recv()
	if err != nil {
		return nil, contextErrorOverride(ctx, err)
	}
	if !resp.GetMeta().GetOK() {
		return nil, errors.New(resp.GetMeta().GetErrorMsg())
	}

	w := &Watcher{
		ch:     make(chan WatchEvent, 1),
		errs:   make(chan error, 1),
		done:   make(chan struct{}),
		stream: stream,
	}

	// start watcher loop
	go func() {
		defer close(w.ch)
		for {
			// wait until the stream sends a message
			resp, err := stream.Recv()
			if err != nil {
				w.errs <- contextErrorOverride(ctx, err)
				return
			}
			if !resp.GetMeta().GetOK() {
				w.errs <- errors.New(resp.GetMeta().GetErrorMsg())
				return
			}

			select {
			case <-w.done:
				w.errs <- nil
				return
			case w.ch <- WatchEvent{
				Topic:     resp.GetTopic(),
				MinOffset: resp.GetMinOffset(),
				MaxOffset: resp.GetMaxOffset(),
			}:
			}
		}
	}()

	return w, nil
}

// Events returns WatchEvents, it is recommended to call in a loop
func (w *Watcher) Events() <-chan WatchEvent {
	return w.ch
}

// Close stops the watcher
func (w *Watcher) Close() error {
	close(w.done)
	// best effort send close request
	_ = w.stream.Send(&protocol.WatchRequest{
		Term: true,
	})
	_ = w.stream.CloseSend()
	return <-w.errs
}
