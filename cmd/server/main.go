package main

import (
	"log"
	"net/http"

	"github.com/haraqa/haraqa/server"
)

func main() {
	s, err := server.NewServer()
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(http.ListenAndServe(":8000", s))
}
