package broker

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"os"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

func TestListen(t *testing.T) {
	t.Run("Errors", func(t *testing.T) {
		b := &Broker{
			GRPCServer: &grpc.Server{},
		}
		ctx := context.Background()
		err := b.Listen(ctx)
		if !os.IsNotExist(errors.Cause(err)) {
			t.Fatal(err)
		}

		longSock := make([]byte, 1025)
		_, err = rand.Read(longSock[:])
		if err != nil {
			t.Fatal(err)
		}
		b.UnixSocket = base64.StdEncoding.EncodeToString(longSock)
		err = b.Listen(ctx)
		if strings.HasPrefix(errors.Cause(err).Error(), "failed to listen on unix socket") {
			t.Fatal(err)
		}

		b.UnixSocket = ".haraqa.tmp.haraqa.sock"
		err = b.Listen(ctx)
		if errors.Cause(err) != grpc.ErrServerStopped {
			t.Fatal(err)
		}

		b.GRPCPort = 70000
		err = b.Listen(ctx)
		if errors.Cause(err).Error() != "listen tcp: address 70000: invalid port" {
			t.Fatal(err)
		}

		b.DataPort = 70000
		err = b.Listen(ctx)
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
}
