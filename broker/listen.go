package broker

import (
	"context"
	"net"

	"github.com/pkg/errors"
)

// Listen starts accpting connection on the given ports. It blocks until an error occurs
func (b *Broker) Listen(ctx context.Context) error {
	errs := make(chan error, 3)
	defer func() {
		b.dataListener.Close()
		b.grpcListener.Close()
		b.unixListener.Close()
	}()

	// serve file data
	go func() {
		for {
			conn, err := b.dataListener.Accept()
			if err != nil {
				errs <- errors.Wrap(err, "failed to serve tcp data connection")
				return
			}
			f, err := conn.(*net.TCPConn).File()
			conn.Close()
			if err != nil {
				b.logger.Println(errors.Wrap(err, "unable to get tcp connection file"))
				continue
			}
			go b.handleDataConn(f)
		}
	}()
	go func() {
		for {
			conn, err := b.unixListener.Accept()
			if err != nil {
				errs <- errors.Wrap(err, "failed to serve unix file data connection")
				return
			}
			f, err := conn.(*net.UnixConn).File()
			conn.Close()
			if err != nil {
				b.logger.Println(errors.Wrap(err, "unable to get unix connection file"))
				continue
			}
			go b.handleDataConn(f)
		}
	}()

	// serve grpc
	go func() {
		if err := b.GRPCServer.Serve(b.grpcListener); err != nil {
			errs <- errors.Wrap(err, "failed to serve")
			return
		}
		errs <- nil
	}()

	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
