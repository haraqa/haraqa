package main

import (
	"bytes"
	_ "embed"
	"flag"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"

	"github.com/haraqa/haraqa/pkg/server"
)

var (
	//go:embed docs.html
	docsHTML []byte
	//go:embed swagger.html
	swaggerHTML []byte
	//go:embed redocs.html
	redocHTML []byte
	//go:embed swagger.yaml
	swaggerYAML []byte
)

func main() {
	// replace github url with relative
	gitURL := []byte("https://raw.githubusercontent.com/haraqa/haraqa/master/cmd/server/")
	const docPath = "/docs/"
	swaggerHTML = bytes.ReplaceAll(swaggerHTML, gitURL, []byte(docPath))
	redocHTML = bytes.ReplaceAll(redocHTML, gitURL, []byte(docPath))

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

	// setup logger
	logger := logrus.New()

	// set a ballast
	if ballastSize >= 0 {
		_ = make([]byte, ballastSize)
	}

	// check args
	if flag.NArg() == 0 {
		logger.Fatal("Missing directory args")
	}

	// get options
	var opts []server.Option
	opts = append(opts, server.WithDefaultQueue(flag.Args(), fileCache, fileEntries))
	opts = append(opts, server.WithLogger(logger))
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
				// we're already using a canonical mime header format, so we don't need to use w.Header().Set()
				w.Header()["Access-Control-Allow-Origin"] = []string{"*"}
				next.ServeHTTP(w, r)
			})
		}))
	}
	if docs {
		http.Handle(docPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch strings.TrimPrefix(r.URL.Path, docPath) {
			case "redocs", "redocs.html":
				_, _ = w.Write(redocHTML)
			case "swagger", "swagger.html":
				_, _ = w.Write(swaggerHTML)
			case "swagger.yaml":
				_, _ = w.Write(swaggerYAML)
			default:
				_, _ = w.Write(docsHTML)
			}
		}))
	}

	// create a server
	s, err := server.NewServer(opts...)
	if err != nil {
		logger.Fatal(err)
	}
	http.Handle("/", s)

	// listen
	logger.Println("Listening on port", httpPort)
	logger.Fatal(http.ListenAndServe(":"+strconv.FormatUint(uint64(httpPort), 10), nil))
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
