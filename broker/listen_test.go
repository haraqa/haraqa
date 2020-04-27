package broker

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/pkg/errors"
)

func TestListen(t *testing.T) {
	t.Run("Errors", func(t *testing.T) {
		// invalid unix socket
		_, err := NewBroker(WithUnixSocket("", os.ModePerm), WithDataPort(0), WithGRPCPort(0))
		if !os.IsNotExist(errors.Cause(err)) {
			t.Fatal(err)
		}

		// invalid unix socket
		longSock := make([]byte, 1025)
		_, err = rand.Read(longSock[:])
		if err != nil {
			t.Fatal(err)
		}
		_, err = NewBroker(WithUnixSocket(base64.StdEncoding.EncodeToString(longSock), os.ModePerm), WithDataPort(0), WithGRPCPort(0))
		if !strings.HasPrefix(err.Error(), "failed to listen on unix socket") {
			t.Fatal(err)
		}

		// invalid data port
		_, err = NewBroker(WithDataPort(70000), WithGRPCPort(0))
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
		b, err := NewBroker(WithGRPCPort(0), WithDataPort(0), WithUnixSocket(".haraqa.listen.sock", 0600))
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
		b, err := NewBroker(WithGRPCPort(0), WithDataPort(0), WithUnixSocket(".haraqa.listen.connect.sock", 0600))
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

		unixData, err := net.Dial("unix", ".haraqa.listen.connect.sock")
		if err != nil {
			t.Fatal(err)
		}
		defer unixData.Close()

		cancel()
		err = <-listenResult
		if err != nil {
			t.Fatal(err)
		}
	})
}
