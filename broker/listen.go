package broker

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// Listen starts a new grpc server & listener on the given ports
func (b *Broker) Listen(ctx context.Context) error {
	errs := make(chan error, 3)

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
		if strings.HasSuffix(err.Error(), "bind: address already in use") {
			// common issue is reopening a socket if broker didn't close properly, remove and retry
			if info, e := os.Stat(b.config.UnixSocket); e == nil {
				if !info.IsDir() {
					_ = os.RemoveAll(b.config.UnixSocket)
					unixListener, err = net.Listen("unix", b.config.UnixSocket)
				}
			}
		}

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
			f, err := conn.(*net.TCPConn).File()
			conn.Close()
			if err != nil {
				log.Println(errors.Wrap(err, "unable to get tcp connection file"))
				continue
			}
			go b.handleDataConn(f)
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
	go func() {
		if err := b.GRPCServer.Serve(lis); err != nil {
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
