package broker

import (
	"os"
	"sync"

	"github.com/haraqa/haraqa/internal/protocol"
	"github.com/haraqa/haraqa/internal/queue"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var DefaultConfig = Config{
	Volumes:         []string{".haraqa"},
	ConsumePoolSize: 10,
	MaxEntries:      250000,
	GRPCPort:        4353,
	DataPort:        14353,
	UnixSocket:      "/tmp/haraqa.sock",
	UnixMode:        0600,
}

type Config struct {
	Volumes         []string
	ConsumePoolSize uint64
	MaxEntries      int
	GRPCPort        uint
	DataPort        uint
	UnixSocket      string
	UnixMode        os.FileMode
}

type Broker struct {
	protocol.UnimplementedHaraqaServer
	s          *grpc.Server
	config     Config
	Q          queue.Queue
	listenWait sync.WaitGroup
}

// NewBroker creates a new instance of the haraqa grpc server
func NewBroker(config Config) (*Broker, error) {
	//TODO: validate volumes
	if len(config.Volumes) == 0 {
		return nil, errors.New("missing volumes in config")
	}

	q, err := queue.NewQueue(config.Volumes, config.MaxEntries, config.ConsumePoolSize)
	if err != nil {
		return nil, err
	}

	return &Broker{
		config: config,
		Q:      q,
	}, nil
}

// Close attempts to gracefully close all connections and the server
func (b *Broker) Close() error {
	if b.s != nil {
		b.s.GracefulStop()
	}

	b.listenWait.Wait()

	return nil
}
