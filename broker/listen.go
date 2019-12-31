package broker

import (
	"net"
	"os"
	"strconv"

	pb "github.com/haraqa/haraqa/protocol"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var DefaultConfig = Config{
	Volumes:    []string{".haraqa"},
	MaxEntries: 20000,
	GRPCPort:   4353,
	StreamPort: 14353,
	UnixSocket: "/tmp/haraqa.sock",
	UnixMode:   0600,
}

type Config struct {
	Volumes    []string
	Queue      Queue
	MaxEntries int
	GRPCPort   uint
	StreamPort uint
	UnixSocket string
	UnixMode   os.FileMode
}

type Broker struct {
	pb.UnimplementedHaraqaServer
	s       *grpc.Server
	config  Config
	streams streams
}

// NewBroker creates a new instance of the haraqa grpc server
func NewBroker(config Config) (*Broker, error) {
	//TODO: validate volumes
	if len(config.Volumes) == 0 {
		return nil, errors.New("missing volumes in config")
	}

	if config.Queue == nil {
		var err error
		config.Queue, err = NewQueue(config.Volumes, config.MaxEntries)
		if err != nil {
			return nil, err
		}
	}

	return &Broker{
		config: config,
		streams: streams{
			m: make(map[string]chan stream),
		},
	}, nil
}

// Close attempts to gracefully close all connections and the server
func (b *Broker) Close() error {
	if b.s != nil {
		b.s.GracefulStop()
	}

	b.streams.Lock()
	defer b.streams.Unlock()
	for k, ch := range b.streams.m {
		close(ch)
		delete(b.streams.m, k)
	}

	return nil
}

// Listen starts a new grpc server on the given port
func (b *Broker) Listen() error {
	errs := make(chan error, 2)

	// open tcp file stream port
	streamListener, err := net.Listen("tcp", ":"+strconv.FormatUint(uint64(b.config.StreamPort), 10))
	if err != nil {
		return errors.Wrapf(err, "failed to listen on stream port %d", b.config.StreamPort)
	}
	defer streamListener.Close()

	// open grpc port
	lis, err := net.Listen("tcp", ":"+strconv.FormatUint(uint64(b.config.GRPCPort), 10))
	if err != nil {
		return errors.Wrapf(err, "failed to listen on grpc port %d", b.config.GRPCPort)
	}
	defer lis.Close()

	// open unix file stream
	unixListener, err := net.Listen("unix", b.config.UnixSocket)
	if err != nil {
		os.RemoveAll(b.config.UnixSocket)
		unixListener, err = net.Listen("unix", b.config.UnixSocket)
		if err != nil {
			return errors.Wrapf(err, "failed to listen on unix socket %s", b.config.UnixSocket)
		}
	}
	defer unixListener.Close()
	if err = os.Chmod(b.config.UnixSocket, b.config.UnixMode); err != nil {
		return errors.Wrap(err, "unable to open unix socket to all users")
	}

	// serve file stream
	go func() {
		for {
			conn, err := streamListener.Accept()
			if err != nil {
				errs <- errors.Wrap(err, "failed to serve tcp file stream")
				return
			}
			go b.handleStream(conn.(*net.TCPConn))
		}
	}()
	go func() {
		for {
			conn, err := unixListener.Accept()
			if err != nil {
				errs <- errors.Wrap(err, "failed to serve unix file stream")
				return
			}
			go b.handleStream(conn.(*net.UnixConn))
		}
	}()

	// serve grpc
	grpcServer := grpc.NewServer()
	pb.RegisterHaraqaServer(grpcServer, b)
	b.s = grpcServer
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			errs <- errors.Wrap(err, "failed to serve")
			return
		}
		errs <- nil
	}()

	return <-errs
}
