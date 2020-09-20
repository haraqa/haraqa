package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"strconv"

	"github.com/haraqa/haraqa/pkg/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	var (
		ballastSize  int64
		httpPort     uint
		fileCache    bool
		fileEntries  int64
		promEnabled  bool
		consumeLimit int64
		cors         bool
		docs         bool
	)
	flag.Int64Var(&ballastSize, "ballast", 1<<30, "Garbage collection ballast")
	flag.UintVar(&httpPort, "http", 4353, "Port to listen on")
	flag.BoolVar(&fileCache, "cache", true, "Enable queue file caching")
	flag.Int64Var(&fileEntries, "entries", 5000, "The number of msg entries per queue file")
	flag.Int64Var(&consumeLimit, "limit", -1, "Default batch limit for consumers")
	flag.BoolVar(&promEnabled, "prometheus", true, "Enable prometheus metrics")
	flag.BoolVar(&cors, "cors", true, "Enable CORS")
	flag.BoolVar(&docs, "docs", true, "Enable Docs pages")
	flag.Parse()

	// set a ballast
	if ballastSize >= 0 {
		_ = make([]byte, ballastSize)
	}

	// check args
	if flag.NArg() == 0 {
		log.Fatal("Missing directory args")
	}

	// get options
	var opts []server.Option
	opts = append(opts, server.WithFileQueue(flag.Args(), fileCache, fileEntries))
	if consumeLimit > 0 {
		opts = append(opts, server.WithDefaultConsumeLimit(consumeLimit))
	}
	if promEnabled {
		// setup prometheus metrics
		middleware, metrics := promMetrics()
		http.Handle("/metrics", promhttp.Handler())
		opts = append(opts, server.WithMiddleware(middleware), server.WithMetrics(metrics))
	}
	if cors {
		opts = append(opts, server.WithMiddleware(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Access-Control-Allow-Origin", "*")
				next.ServeHTTP(w, r)
			})
		}))
	}
	if docs {
		http.Handle("/docs/swagger.yaml", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, "swagger.yaml")
		}))
		http.Handle("/docs/swagger", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, "swagger.html")
		}))
		http.Handle("/docs", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, "redocs.html")
		}))
	}

	// create a server
	s, err := server.NewServer(opts...)
	if err != nil {
		log.Fatal(err)
	}
	http.Handle("/", s)

	// listen
	log.Println("Listening on port", httpPort)
	log.Fatal(http.ListenAndServe(":"+strconv.FormatUint(uint64(httpPort), 10), nil))
}

func promMetrics() (func(http.Handler) http.Handler, *Metrics) {
	inFlightGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "in_flight_requests",
		Help: "A gauge of requests currently being served by the wrapped handler.",
	})
	counter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "api_requests_total",
			Help: "A counter for requests to the wrapped handler.",
		},
		[]string{"code", "method"},
	)
	duration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "request_duration_seconds",
			Help:    "A histogram of latencies for requests.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"code", "method"},
	)
	requestSize := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "request_size_bytes",
			Help:    "A histogram of request sizes for requests.",
			Buckets: []float64{200, 500, 900, 1500},
		},
		[]string{"code", "method"},
	)
	responseSize := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "response_size_bytes",
			Help:    "A histogram of response sizes for requests.",
			Buckets: []float64{200, 500, 900, 1500},
		},
		[]string{"code", "method"},
	)
	produceBatchSize := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "produce_batch_size",
			Help:    "A histogram of batch sizes for produce requests.",
			Buckets: []float64{10, 50, 100, 200, 500, 1000, 2000},
		},
	)
	consumeBatchSize := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "consume_batch_size",
			Help:    "A histogram of batch sizes for consume requests.",
			Buckets: []float64{10, 50, 100, 200, 500, 1000, 2000},
		},
	)

	// Register all of the metrics in the standard registry.
	prometheus.MustRegister(inFlightGauge, counter, duration, requestSize, responseSize, produceBatchSize, consumeBatchSize)

	return func(next http.Handler) http.Handler {
			return promhttp.InstrumentHandlerInFlight(inFlightGauge,
				promhttp.InstrumentHandlerDuration(duration,
					promhttp.InstrumentHandlerRequestSize(requestSize,
						promhttp.InstrumentHandlerResponseSize(responseSize,
							promhttp.InstrumentHandlerCounter(counter,
								next,
							),
						),
					),
				),
			)
		}, &Metrics{
			produceHist: produceBatchSize,
			consumeHist: consumeBatchSize,
		}
}

// Metrics is a prometheus based implementation of the haraqa Metrics interface
type Metrics struct {
	produceHist prometheus.Histogram
	consumeHist prometheus.Histogram
}

// ProduceMsgs updates the produce histogram with the batch size
func (m *Metrics) ProduceMsgs(n int) {
	m.produceHist.Observe(float64(n))
}

// ConsumeMsgs updates the consume histogram with the batch size
func (m *Metrics) ConsumeMsgs(n int) {
	m.consumeHist.Observe(float64(n))
}
