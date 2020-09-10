package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/haraqa/haraqa/pkg/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	var (
		ballastSize int64
		httpPort    uint
	)
	flag.Int64Var(&ballastSize, "ballast", 1<<30, "Garbage collection ballast")
	flag.UintVar(&httpPort, "http", 4353, "Port to listen on")
	flag.Parse()

	// set a ballast
	if ballastSize >= 0 {
		_ = make([]byte, ballastSize)
	}

	// setup metrics
	middleware := promMetrics()
	http.Handle("/metrics", promhttp.Handler())

	// create a server
	s, err := server.NewServer(server.WithMiddleware(middleware))
	if err != nil {
		log.Fatal(err)
	}

	// listen
	http.Handle("/", s)
	log.Println("Listening on port", httpPort)
	log.Fatal(http.ListenAndServe(":"+strconv.FormatUint(uint64(httpPort), 10), nil))
}

func promMetrics() mux.MiddlewareFunc {
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

	// Register all of the metrics in the standard registry.
	prometheus.MustRegister(inFlightGauge, counter, duration, requestSize, responseSize)

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
	}
}
