package broker

import (
	"os"
	"sync"
	"time"

	"github.com/haraqa/haraqa/internal/protocol"
	"github.com/haraqa/haraqa/internal/queue"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

// DefaultConfig is a default configuration for a broker writing to a single volume
//  '.haraqa'
var DefaultConfig = Config{
	Volumes:         []string{".haraqa"},
	ConsumePoolSize: 10,
	MaxEntries:      250000,
	GRPCPort:        4353,
	DataPort:        14353,
	UnixSocket:      "/tmp/haraqa.sock",
	UnixMode:        0600,
}

// Config is the configuration for a new Broker
type Config struct {
	Volumes         []string
	ConsumePoolSize uint64
	MaxEntries      int
	GRPCPort        uint
	DataPort        uint
	UnixSocket      string
	UnixMode        os.FileMode
	GRPCServer      *grpc.Server
}

// Broker is core structure of a haraqa broker, it listens for new grpc and data
//  connections and shepherds data going into and out of a persistent queue
type Broker struct {
	protocol.UnimplementedHaraqaServer
	config     Config
	Q          queue.Queue
	listenWait sync.WaitGroup

	groupMux   sync.Mutex
	groupLocks map[string]chan struct{}
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

	b := &Broker{
		config:     config,
		Q:          q,
		groupLocks: make(map[string]chan struct{}),
	}
	if b.config.GRPCServer == nil {
		b.config.GRPCServer = grpc.NewServer()
	}
	protocol.RegisterHaraqaServer(b.config.GRPCServer, b)
	return b, nil
}

// Close attempts to gracefully close all connections and the server
func (b *Broker) Close() error {
	if b.config.GRPCServer != nil {
		b.config.GRPCServer.GracefulStop()
	}

	b.listenWait.Wait()

	return nil
}

func (b *Broker) getGroupLock(group []byte, t *time.Timer) bool {
	b.groupMux.Lock()
	ch, ok := b.groupLocks[string(group)]
	if !ok {
		ch = make(chan struct{}, 1)
		b.groupLocks[string(group)] = ch
	}
	b.groupMux.Unlock()

	select {
	case ch <- struct{}{}:
		// lock acquired
		return true
	case <-t.C:
		// lock not acquired
		return false
	}
}

func (b *Broker) releaseGroupLock(group []byte) {
	b.groupMux.Lock()
	ch, ok := b.groupLocks[string(group)]
	b.groupMux.Unlock()
	if !ok {
		return
	}

	// unlock if locked
	select {
	case <-ch:
	default:
	}
}
