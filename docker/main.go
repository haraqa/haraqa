package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"strconv"

	"github.com/haraqa/haraqa/broker"
)

func main() {
	config := broker.DefaultConfig

	var (
		ballast   int64
		pprofPort uint
	)
	flag.Int64Var(&ballast, "ballast", 1<<30, "Garbage collection ballast")
	flag.UintVar(&pprofPort, "pprof", 6060, "Port for serving pprof metrics")
	flag.UintVar(&config.GRPCPort, "grpc", config.GRPCPort, "Port to listen on for grpc connections")
	flag.UintVar(&config.StreamPort, "stream", config.StreamPort, "Port to listen on for stream connections")
	flag.StringVar(&config.UnixSocket, "unix", config.UnixSocket, "Unix socket for local stream connections")
	flag.Parse()

	// set a ballast
	_ = make([]byte, ballast)

	go func() {
		log.Println(http.ListenAndServe(":"+strconv.FormatUint(uint64(pprofPort), 10), nil))
	}()

	// get volumes from args
	config.Volumes = flag.Args()

	log.Printf("config: %+v\n", config)
	b, err := broker.NewBroker(config)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Listening on ports %d (grpc) and %d (stream) and unix socket %s (stream)\n", config.GRPCPort, config.StreamPort, config.UnixSocket)
	if err := b.Listen(); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
