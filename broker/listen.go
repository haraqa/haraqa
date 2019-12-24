package broker

import (
	"net"

	pb "github.com/haraqa/haraqa/protocol"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var DefaultConfig = Config{
	Volumes:    []string{".haraqa"},
	MaxEntries: 20000,
}

type Config struct {
	Volumes    []string
	Queue      Queue
	MaxEntries int
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
func (b *Broker) Listen(grpcPort, streamPort string) error {
	errs := make(chan error, 1)

	// open tcp file stream port
	streamListener, err := net.Listen("tcp", streamPort)
	if err != nil {
		return errors.Wrap(err, "failed to listen")
	}
	defer streamListener.Close()

	// open grpc port
	lis, err := net.Listen("tcp", grpcPort)
	if err != nil {
		return errors.Wrap(err, "failed to listen")
	}
	defer lis.Close()

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

	select {
	case err := <-errs:
		return err
	}
}
