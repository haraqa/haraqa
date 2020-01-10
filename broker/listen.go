package broker

import (
	"net"
	"os"
	"strconv"
	"sync"

	"github.com/haraqa/haraqa/internal/protocol"
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
	Queue           Queue
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
	listenWait sync.WaitGroup
}

// NewBroker creates a new instance of the haraqa grpc server
func NewBroker(config Config) (*Broker, error) {
	//TODO: validate volumes
	if len(config.Volumes) == 0 {
		return nil, errors.New("missing volumes in config")
	}

	if config.Queue == nil {
		var err error
		config.Queue, err = NewQueue(config.Volumes, config.MaxEntries, config.ConsumePoolSize)
		if err != nil {
			return nil, err
		}
	}

	return &Broker{
		config: config,
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

// Listen starts a new grpc server on the given port
func (b *Broker) Listen() error {
	b.listenWait.Add(1)
	defer b.listenWait.Done()
	errs := make(chan error, 2)

	// open tcp file data port
	dataListener, err := net.Listen("tcp", ":"+strconv.FormatUint(uint64(b.config.DataPort), 10))
	if err != nil {
		return errors.Wrapf(err, "failed to listen on data port %d", b.config.DataPort)
	}
	defer dataListener.Close()

	// open grpc port
	lis, err := net.Listen("tcp", ":"+strconv.FormatUint(uint64(b.config.GRPCPort), 10))
	if err != nil {
		return errors.Wrapf(err, "failed to listen on grpc port %d", b.config.GRPCPort)
	}
	defer lis.Close()

	// open unix file data listener
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

	// serve file data
	go func() {
		for {
			conn, err := dataListener.Accept()
			if err != nil {
				errs <- errors.Wrap(err, "failed to serve tcp data connection")
				return
			}
			go b.handleDataConn(conn.(*net.TCPConn))
		}
	}()
	go func() {
		for {
			conn, err := unixListener.Accept()
			if err != nil {
				errs <- errors.Wrap(err, "failed to serve unix file data connection")
				return
			}
			go b.handleDataConn(conn.(*net.UnixConn))
		}
	}()

	// serve grpc
	grpcServer := grpc.NewServer()
	protocol.RegisterHaraqaServer(grpcServer, b)
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
