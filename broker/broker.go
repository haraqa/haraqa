package broker

import (
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
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
	DefaultGRPCPort        = 4353
	DefaultDataPort        = 14353
	DefaultUnixSocket      = "/tmp/haraqa.sock"
	DefaultUnixMode        = os.FileMode(0600)
	DefaultLogger          = log.New(ioutil.Discard, "", 0)
	DefaultGRPCOptions     = []grpc.ServerOption{}
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
	GRPCPort        int                 // port on which to listen for grpc connections
	DataPort        int                 // port on which to listen for data connections
	UnixSocket      string              // unixfile on which to listen for a local data connection
	UnixMode        os.FileMode         // file mode of unixfile
	grpcOptions     []grpc.ServerOption // options to start the grpc server with
	logger          *log.Logger         // logger to print error messages to

	// listeners
	grpcListener net.Listener
	dataListener net.Listener
	unixListener net.Listener

	groupMux   sync.Mutex
	groupLocks map[string]chan struct{}
}

// NewBroker creates a new instance of the haraqa broker and binds to the given ports
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
		logger:          DefaultLogger,
		grpcOptions:     DefaultGRPCOptions,
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

	// open unix file data listener
	b.unixListener, err = net.Listen("unix", b.UnixSocket)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to listen on unix socket %s", b.UnixSocket)
	}
	if err = os.Chmod(b.UnixSocket, b.UnixMode); err != nil {
		b.unixListener.Close()
		return nil, errors.Wrap(err, "unable to open unix socket to all users")
	}

	// open tcp file data port
	b.dataListener, err = net.Listen("tcp", ":"+strconv.Itoa(b.DataPort))
	if err != nil {
		b.unixListener.Close()
		return nil, errors.Wrapf(err, "failed to listen on data port %d", b.DataPort)
	}
	if b.DataPort == 0 {
		b.DataPort = b.dataListener.Addr().(*net.TCPAddr).Port
	}

	// initialize grpc
	b.GRPCServer = grpc.NewServer(b.grpcOptions...)
	protocol.RegisterHaraqaServer(b.GRPCServer, b)
	b.grpcListener, err = net.Listen("tcp", ":"+strconv.Itoa(b.GRPCPort))
	if err != nil {
		b.unixListener.Close()
		b.dataListener.Close()
		return nil, errors.Wrapf(err, "failed to listen on grpc port %d", b.GRPCPort)
	}
	if b.GRPCPort == 0 {
		b.GRPCPort = b.grpcListener.Addr().(*net.TCPAddr).Port
	}

	return b, nil
}

// Option is used with NewBroker to set parameters and override defaults
type Option func(*Broker) error

// WithLogger sets a logger to print out connection information and errors
func WithLogger(logger *log.Logger) Option {
	return func(b *Broker) error {
		if logger == nil {
			return errors.New("invalid logger")
		}
		b.logger = logger
		return nil
	}
}

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
func WithGRPCPort(n int) Option {
	return func(b *Broker) error {
		b.GRPCPort = n
		return nil
	}
}

// WithDataPort sets the port on which to listen for data connections
func WithDataPort(n int) Option {
	return func(b *Broker) error {
		b.DataPort = n
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
