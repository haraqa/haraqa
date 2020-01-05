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
		ballast    int64
		httpPort   uint
		fileserver bool
	)
	flag.Int64Var(&ballast, "ballast", 1<<30, "Garbage collection ballast")
	flag.UintVar(&httpPort, "http", 6060, "Port for serving pprof metrics and files")
	flag.BoolVar(&fileserver, "fileserver", true, "If true, files are served at http port")
	flag.UintVar(&config.GRPCPort, "grpc", config.GRPCPort, "Port to listen on for grpc connections")
	flag.UintVar(&config.DataPort, "data", config.DataPort, "Port to listen on for data connections")
	flag.StringVar(&config.UnixSocket, "unix", config.UnixSocket, "Unix socket for local data connections")
	flag.Parse()

	// set a ballast
	_ = make([]byte, ballast)

	// get volumes from args
	config.Volumes = flag.Args()

	go func() {
		if fileserver {
			http.Handle("/topics/", http.StripPrefix("/topics/", http.FileServer(http.Dir(config.Volumes[len(config.Volumes)-1]))))
		}
		log.Println(http.ListenAndServe(":"+strconv.FormatUint(uint64(httpPort), 10), nil))
	}()

	log.Printf("config: %+v\n", config)
	b, err := broker.NewBroker(config)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Listening on ports %d (grpc) and %d (data) and unix socket %s (data)\n", config.GRPCPort, config.DataPort, config.UnixSocket)
	if err := b.Listen(); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
