package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/haraqa/haraqa/broker"
)

func main() {
	var ballast int64
	flag.Int64Var(&ballast, "ballast", 1<<30, "Garbage collection ballast")
	flag.Parse()

	// set a ballast
	_ = make([]byte, ballast)

	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	config := broker.DefaultConfig
	if len(os.Args) > 1 {
		config.Volumes = flag.Args()[1:]
	}
	log.Println("config:", config)
	b, err := broker.NewBroker(config)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Listening on ports 4353 (grpc) and 14353 (stream)")
	if err := b.Listen(":4353", ":14353"); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
