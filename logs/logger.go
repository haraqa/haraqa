package logs

import (
	"context"
	"io"
	"log"

	"github.com/haraqa/haraqa"
	"github.com/haraqa/haraqa/protocol"
	"github.com/pkg/errors"
)

type Logger struct {
	*log.Logger
	client *haraqa.Client
	ch     chan haraqa.ProduceMsg
	done   chan struct{}
}

func NewLogger(l *log.Logger, config haraqa.Config, topic []byte) (*Logger, error) {
	client, err := haraqa.NewClient(config)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	err = client.CreateTopic(ctx, topic)
	if err != nil && errors.Cause(err) != protocol.ErrTopicExists {
		return nil, err
	}

	ch := make(chan haraqa.ProduceMsg, 4096)
	logger := &Logger{
		Logger: l,
		ch:     ch,
		done:   make(chan struct{}),
		client: client,
	}
	go logger.alwaysProduce(topic)

	w := &writer{ch: ch}
	logger.Logger.SetOutput(io.MultiWriter(w, l.Writer()))

	return logger, nil
}

func (l *Logger) Close() {
	close(l.ch)
	<-l.done
}

func (l *Logger) alwaysProduce(topic []byte) {
	ctx := context.Background()

	for {
		err := l.client.ProduceStream(ctx, topic, l.ch)
		if err == nil {
			l.client.Close()
			close(l.done)
			return
		}
	}
}

// writer implements io.Writer
type writer struct {
	ch chan haraqa.ProduceMsg
}

func (w *writer) Write(b []byte) (int, error) {
	// log.Logger reuses a buffer, so we need to make a copy of our message
	msg := make([]byte, len(b))
	copy(msg, b)
	w.ch <- haraqa.ProduceMsg{Msg: msg}
	return len(b), nil
}
