package logs

import (
	"context"
	"io"
	"log"
	"os"

	"github.com/haraqa/haraqa"
	"github.com/pkg/errors"
)

// ExampleLogger is an example of the logger implementation
func ExampleLogger() {
	logger := log.New(os.Stderr, "ERROR", log.LstdFlags)
	logErr, err := NewLogger(logger, []byte("Errors"))
	if err != nil {
		panic(err)
	}
	// Close should be called to flush any messages before exiting
	defer logErr.Close()

	logErr.Println("Some log here")
	logErr.Println("Another log here")
}

// Logger implements log.Logger with a writer that writes to haraqa
type Logger struct {
	*log.Logger
	client   *haraqa.Client
	producer *haraqa.Producer
	done     chan struct{}
}

// NewLogger connects to haraqa and returns a new *Logger
func NewLogger(l *log.Logger, topic []byte) (*Logger, error) {
	client, err := haraqa.NewClient()
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	err = client.CreateTopic(ctx, topic)
	if err != nil && errors.Cause(err) != haraqa.ErrTopicExists {
		return nil, err
	}

	producer, err := client.NewProducer(haraqa.WithTopic(topic), haraqa.WithIgnoreErrors())
	if err != nil {
		return nil, err
	}

	logger := &Logger{
		Logger:   l,
		producer: producer,
		done:     make(chan struct{}),
		client:   client,
	}

	w := &writer{producer: producer}
	logger.Logger.SetOutput(io.MultiWriter(w, l.Writer()))

	return logger, nil
}

// Close closes the haraqa connection and halts the logger
func (l *Logger) Close() {
	l.producer.Close()
	<-l.done
}

// writer implements io.Writer
type writer struct {
	producer *haraqa.Producer
}

func (w *writer) Write(b []byte) (int, error) {
	// log.Logger reuses a buffer, so we need to make a copy of our message
	msg := make([]byte, len(b))
	copy(msg, b)
	return len(b), w.producer.Send(msg)
}
