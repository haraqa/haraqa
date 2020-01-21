package main

import (
	"bytes"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"github.com/haraqa/haraqa"
)

func handler(w http.ResponseWriter, r *http.Request) {
	// get topic from path
	topic := []byte(r.URL.Path[1:])
	if len(topic) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("missing topic"))
		return
	}

	// allow for sharding
	config := haraqa.DefaultConfig
	config.Host = getBrokerByTopic(topic)

	// open a new client for this topic
	client, err := haraqa.NewClient(config)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}
	defer client.Close()

	// perform different actions for each method
	switch r.Method {
	case http.MethodDelete:
		err := client.DeleteTopic(r.Context(), topic)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

	case http.MethodGet:
		// get headers
		offset, _ := strconv.ParseInt(r.Header.Get("OFFSET"), 10, 64)
		limit, err := strconv.ParseInt(r.Header.Get("MAX"), 10, 64)
		if err != nil || limit < 1 {
			limit = 100
		}

		// Send consume request
		msgs, err := client.Consume(r.Context(), topic, offset, limit, nil)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		// Write batch to client
		_, err = w.Write(bytes.Join(msgs, []byte{}))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

	case http.MethodPost:
		defer r.Body.Close()
		// read body into buf
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		}

		// split the body by newlines into multiple messages
		msgs := bytes.SplitAfter(b, []byte("\n"))

		// produce the messages as a batch
		err = client.Produce(r.Context(), topic, msgs...)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		}
	}
}

// getBrokerByTopic returns a broker's host ip based on the topic
// since an infinite number of topics could be used, this function would be
// where you'd place your sharding logic.
func getBrokerByTopic(topic []byte) string {
	//In this example, we just have a single, local shard
	return "127.0.0.1"
}

func main() {
	http.HandleFunc("/", handler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
