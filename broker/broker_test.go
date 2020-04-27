package broker

import (
	"testing"

	"google.golang.org/grpc"
)

func TestOptions(t *testing.T) {
	var err error
	b := new(Broker)
	err = WithLogger(nil)(b)
	if err.Error() != "invalid logger" {
		t.Fatal(err)
	}
	err = WithLogger(DefaultLogger)(b)
	if err != nil {
		t.Fatal(err)
	}

	err = WithVolumes(nil)(b)
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

	err = WithGRPCOptions(grpc.MaxRecvMsgSize(200))(b)
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
}
