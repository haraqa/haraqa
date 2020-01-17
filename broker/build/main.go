package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"strconv"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/haraqa/haraqa/broker"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
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
	flag.IntVar(&config.MaxEntries, "maxentries", config.MaxEntries, "Max entries per file")

	flag.Parse()

	// set a ballast
	_ = make([]byte, ballast)

	// get volumes from args
	config.Volumes = flag.Args()
	config.GRPCServer = grpc.NewServer(
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
	)

	log.Printf("config: %+v\n", config)
	b, err := broker.NewBroker(config)
	if err != nil {
		log.Fatal(err)
	}

	// start http server
	grpc_prometheus.Register(config.GRPCServer)
	go func() {
		if fileserver {
			http.Handle("/topics/", http.StripPrefix("/topics/", http.FileServer(http.Dir(config.Volumes[len(config.Volumes)-1]))))
		}
		http.Handle("/metrics", promhttp.Handler())
		log.Println(http.ListenAndServe(":"+strconv.FormatUint(uint64(httpPort), 10), nil))
	}()

	log.Printf("Listening on ports %d (grpc) and %d (data) and unix socket %s (data)\n", config.GRPCPort, config.DataPort, config.UnixSocket)
	if err := b.Listen(); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
