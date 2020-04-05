package broker

import (
	"testing"

	"google.golang.org/grpc"
)

func TestOptions(t *testing.T) {
	b := new(Broker)
	err := WithVolumes(nil)(b)
	if err.Error() != "missing volumes" {
		t.Fatal(err)
	}
	err = WithVolumes([]string{".haraqa", ".haraqa"})(b)
	if err.Error() != "duplicate volume name found \".haraqa\"" {
		t.Fatal(err)
	}
	err = WithVolumes([]string{".haraqa1", ".haraqa2"})(b)
	if err != nil {
		t.Fatal(err)
	}
	if b.Volumes[0] != ".haraqa1" || b.Volumes[1] != ".haraqa2" {
		t.Fatal(b.Volumes)
	}

	_, err = NewBroker(WithVolumes(nil))
	if err.Error() != "missing volumes" {
		t.Fatal(err)
	}

	err = WithConsumersPerTopic(0)(b)
	if err.Error() != "invalid number of consumers per topic" {
		t.Fatal(err)
	}
	err = WithConsumersPerTopic(12)(b)
	if err != nil {
		t.Fatal(err)
	}
	if b.ConsumePoolSize != 12 {
		t.Fatal(b.ConsumePoolSize)
	}

	err = WithMaxEntries(0)(b)
	if err.Error() != "invalid number of max entries" {
		t.Fatal(err)
	}
	err = WithMaxEntries(12)(b)
	if err != nil {
		t.Fatal(err)
	}
	if b.MaxEntries != 12 {
		t.Fatal(b.MaxEntries)
	}

	err = WithMaxSize(0)(b)
	if err.Error() != "invalid max size" {
		t.Fatal(err)
	}
	err = WithMaxSize(12)(b)
	if err != nil {
		t.Fatal(err)
	}
	if b.MaxSize != 12 {
		t.Fatal(b.MaxSize)
	}

	err = WithGRPCOptions(grpc.MaxMsgSize(200))(b)
	if err != nil {
		t.Fatal(err)
	}
	if b.grpcOptions == nil {
		t.Fatal(b.grpcOptions)
	}

	err = WithMetrics(noopMetrics{})(b)
	if err != nil {
		t.Fatal(err)
	}
	if b.M == nil {
		t.Fatal(b.M)
	}

	// WithGRPCPort sets the port on which to listen for grpc connections
	/*  func WithGRPCPort(n uint16) Option {
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
	    }*/
}
