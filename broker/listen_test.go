package broker

import (
	"context"
	"net"
	"strconv"
	"testing"

	"github.com/pkg/errors"
)

func TestListen(t *testing.T) {
	t.Run("Errors", func(t *testing.T) {
		// invalid data port
		_, err := NewBroker(WithDataPort(70000), WithGRPCPort(0))
		if errors.Cause(err).Error() != "listen tcp: address 70000: invalid port" {
			t.Fatal(err)
		}

		// invalid grpc port
		_, err = NewBroker(WithDataPort(0), WithGRPCPort(70000))
		if errors.Cause(err).Error() != "listen tcp: address 70000: invalid port" {
			t.Fatal(err)
		}
	})
	t.Run("context cancel", func(t *testing.T) {
		b, err := NewBroker(WithGRPCPort(0), WithDataPort(0))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err = b.Listen(ctx)
		if err != ctx.Err() {
			t.Fatal(err)
		}
	})
	t.Run("successful", func(t *testing.T) {
		b, err := NewBroker(WithGRPCPort(0), WithDataPort(0))
		if err != nil {
			t.Fatal(err)
		}
		listenResult := make(chan error, 1)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			err = b.Listen(ctx)
			if err != ctx.Err() {
				listenResult <- err
			}
			listenResult <- nil
		}()

		tcpData, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(b.DataPort))
		if err != nil {
			t.Fatal(err)
		}
		defer tcpData.Close()

		cancel()
		err = <-listenResult
		if err != nil {
			t.Fatal(err)
		}
	})
}
