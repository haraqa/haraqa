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

// Default variables for NewBroker()
var (
	DefaultVolumes         = []string{".haraqa"}
	DefaultConsumePoolSize = uint64(10)
	DefaultMaxEntries      = 10000
	DefaultMaxSize         = int64(-1)
	DefaultGRPCPort        = uint(4353)
	DefaultDataPort        = uint(14353)
	DefaultUnixSocket      = "/tmp/haraqa.sock"
	DefaultUnixMode        = os.FileMode(0600)
)

// Broker is core structure of a haraqa broker, it listens for new grpc and data
//  connections and shepherds data going into and out of a persistent queue
type Broker struct {
	protocol.UnimplementedHaraqaServer
	GRPCServer *grpc.Server
	Q          queue.Queue
	M          Metrics

	Volumes         []string            // Volumes to persist messages to in order of which to write
	ConsumePoolSize uint64              // Number of expected consumers per topic
	MaxEntries      int                 // Maximum number of entries per file before creating a new file
	MaxSize         int64               // Maximum message size the broker will accept, if -1 any message size is accepted
	GRPCPort        uint                // port on which to listen for grpc connections
	DataPort        uint                // port on which to listen for data connections
	UnixSocket      string              // unixfile on which to listen for a local data connection
	UnixMode        os.FileMode         // file mode of unixfile
	grpcOptions     []grpc.ServerOption // options to start the grpc server with

	groupMux   sync.Mutex
	groupLocks map[string]chan struct{}
}

// NewBroker creates a new instance of the haraqa grpc server
func NewBroker(options ...Option) (*Broker, error) {
	// apply defaults
	b := &Broker{
		Volumes:         DefaultVolumes,
		ConsumePoolSize: DefaultConsumePoolSize,
		MaxEntries:      DefaultMaxEntries,
		MaxSize:         DefaultMaxSize,
		GRPCPort:        DefaultGRPCPort,
		DataPort:        DefaultDataPort,
		UnixSocket:      DefaultUnixSocket,
		UnixMode:        DefaultUnixMode,
		M:               noopMetrics{},
		groupLocks:      make(map[string]chan struct{}),
	}

	// apply options
	for i := range options {
		err := options[i](b)
		if err != nil {
			return nil, err
		}
	}

	// initialize queue
	q, err := queue.NewQueue(b.Volumes, b.MaxEntries, b.ConsumePoolSize)
	if err != nil {
		return nil, err
	}
	b.Q = q

	// initialize grpc
	b.GRPCServer = grpc.NewServer(b.grpcOptions...)
	protocol.RegisterHaraqaServer(b.GRPCServer, b)
	return b, nil
}

// Option is used with NewBroker to set parameters and override defaults
type Option func(*Broker) error

// WithVolumes sets the volumes the broker persists messages to in order of which to write
func WithVolumes(volumes []string) Option {
	//TODO: validate volumes
	return func(b *Broker) error {
		if len(volumes) == 0 {
			return errors.New("missing volumes")
		}
		v := make(map[string]struct{}, len(volumes))
		for i := range volumes {
			if _, ok := v[volumes[i]]; ok {
				return errors.Errorf("duplicate volume name found %q", volumes[i])
			}
			v[volumes[i]] = struct{}{}
		}

		b.Volumes = volumes
		return nil
	}
}

// WithConsumersPerTopic sets the number of expected consumers per topic.
// More consumers per topic is possible, but this reflects is the number
// of files to keep in the cache per topic
func WithConsumersPerTopic(n uint64) Option {
	return func(b *Broker) error {
		if n == 0 {
			return errors.New("invalid number of consumers per topic")
		}
		b.ConsumePoolSize = n
		return nil
	}
}

// WithMaxEntries sets the maximum number of entries per file before creating a new file
func WithMaxEntries(n int) Option {
	return func(b *Broker) error {
		if n <= 0 {
			return errors.New("invalid number of max entries")
		}
		b.MaxEntries = n
		return nil
	}
}

// WithMaxSize sets the maximum message size the broker will accept, if -1 any message size is accepted
func WithMaxSize(n int64) Option {
	return func(b *Broker) error {
		if n == 0 {
			return errors.New("invalid max size")
		}
		b.MaxSize = n
		return nil
	}
}

// WithGRPCPort sets the port on which to listen for grpc connections
func WithGRPCPort(n uint16) Option {
	return func(b *Broker) error {
		b.GRPCPort = uint(n)
		return nil
	}
}

// WithDataPort sets the port on which to listen for data connections
func WithDataPort(n uint16) Option {
	return func(b *Broker) error {
		b.DataPort = uint(n)
		return nil
	}
}

// WithUnixSocket sets the unixfile on which to listen for a local data connection
func WithUnixSocket(s string, mode os.FileMode) Option {
	return func(b *Broker) error {
		b.UnixSocket = s
		b.UnixMode = mode
		return nil
	}
}

// WithGRPCOptions sets the options to start the grpc server with
func WithGRPCOptions(options ...grpc.ServerOption) Option {
	return func(b *Broker) error {
		b.grpcOptions = options
		return nil
	}
}

// WithMetrics sets the metrics interface for tracking broker data
func WithMetrics(metrics Metrics) Option {
	return func(b *Broker) error {
		b.M = metrics
		return nil
	}
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
