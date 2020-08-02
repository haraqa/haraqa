package main

import (
	"flag"
	"log"
	"net/http"
	"strconv"

	"github.com/haraqa/haraqa/server"
)

func main() {
	var (
		ballast  int64
		httpPort uint
	)
	flag.Int64Var(&ballast, "ballast", 1<<30, "Garbage collection ballast")
	flag.UintVar(&httpPort, "http", 6060, "Port for serving pprof metrics and files")

	flag.Parse()

	// set a ballast
	_ = make([]byte, ballast)

	s, err := server.NewServer()
	if err != nil {
		log.Fatal(err)
	}

	//http.Handle("/metrics", promhttp.Handler())
	http.Handle("/", s)
	log.Fatal(http.ListenAndServe(":"+strconv.FormatUint(uint64(httpPort), 10), nil))
}
