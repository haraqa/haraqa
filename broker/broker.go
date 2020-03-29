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
	MaxEntries:      10000,
	MaxSize:         -1,
	GRPCPort:        4353,
	DataPort:        14353,
	UnixSocket:      "/tmp/haraqa.sock",
	UnixMode:        0600,
}

// Config is the configuration for a new Broker
type Config struct {
	Volumes         []string            // Volumes to persist messages to in order of which to write
	ConsumePoolSize uint64              // Number of expected consumers per topic
	MaxEntries      int                 // Maximum number of entries per file before creating a new file
	MaxSize         int64               // Maximum message size the broker will accept, if -1 any message size is accepted
	GRPCPort        uint                // port on which to listen for grpc connections
	DataPort        uint                // port on which to listen for data connections
	UnixSocket      string              // unixfile on which to listen for a local data connection
	UnixMode        os.FileMode         // file mode of unixfile
	GRPCOptions     []grpc.ServerOption // options to start the grpc server with
}

// Broker is core structure of a haraqa broker, it listens for new grpc and data
//  connections and shepherds data going into and out of a persistent queue
type Broker struct {
	protocol.UnimplementedHaraqaServer
	config     Config
	GRPCServer *grpc.Server
	Q          queue.Queue

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
		GRPCServer: grpc.NewServer(config.GRPCOptions...),
		Q:          q,
		groupLocks: make(map[string]chan struct{}),
	}

	protocol.RegisterHaraqaServer(b.GRPCServer, b)
	return b, nil
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
	ch := b.groupLocks[string(group)]
	b.groupMux.Unlock()

	// unlock if locked
	select {
	case <-ch:
	default:
	}
}
