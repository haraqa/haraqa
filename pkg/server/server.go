package server

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/haraqa/haraqa/internal/filequeue"
	"github.com/haraqa/haraqa/internal/headers"
	"github.com/haraqa/haraqa/internal/memconsumer"
	"github.com/haraqa/haraqa/internal/queue"
	"github.com/pkg/errors"
)

// Option represents a optional function argument to NewServer
type Option func(*Server) error

// WithConsumerManager overrides the default consumer manager
func WithConsumerManager(consumerManager ConsumerManager) Option {
	return func(s *Server) error {
		if consumerManager == nil {
			return errors.New("consumerManager cannot be nil")
		}
		s.consumerManager = consumerManager
		return nil
	}
}

// WithQueue overrides the default file queue
func WithQueue(q Queue) Option {
	return func(s *Server) error {
		if q == nil {
			return errors.New("queue cannot be nil")
		}
		s.q = q
		return nil
	}
}

// WithFileQueue sets the queue
func WithFileQueue(dirs []string, cache bool, entries int64) Option {
	return func(s *Server) error {
		if s.q != nil {
			return nil
		}
		if len(dirs) == 0 {
			return errors.New("at least one directory must be given")
		}
		if entries < 0 {
			return errors.New("invalid entries, value must not be negative")
		}
		var err error
		s.q, err = filequeue.New(cache, entries, dirs...)
		return err
	}
}

// WithDefaultQueue sets the queue
func WithDefaultQueue(dirs []string, cache bool, entries int64) Option {
	return func(s *Server) error {
		if s.q != nil {
			return nil
		}
		if len(dirs) == 0 {
			return errors.New("at least one directory must be given")
		}
		if entries < 0 {
			return errors.New("invalid entries, value must not be negative")
		}
		var err error
		s.q, err = queue.NewQueue(dirs, cache, entries)
		return err
	}
}

// WithMetrics sets the handler for produce and consume metrics
func WithMetrics(metrics Metrics) Option {
	return func(s *Server) error {
		if metrics == nil {
			return errors.New("metrics cannot be nil")
		}
		s.metrics = metrics
		return nil
	}
}

// WithDefaultConsumeLimit sets the default consume limit for clients that consume with limit < 0
func WithDefaultConsumeLimit(n int64) Option {
	return func(s *Server) error {
		if n <= 0 {
			n = -1
		}
		s.defaultConsumeLimit = n
		return nil
	}
}

// WithMiddleware adds the given middleware to the endpoints defined in the http router
func WithMiddleware(middleware ...func(http.Handler) http.Handler) Option {
	return func(s *Server) error {
		s.middlewares = append(s.middlewares, middleware...)
		return nil
	}
}

// WithLogger adds a logger to the server
func WithLogger(logger Logger) Option {
	return func(s *Server) error {
		s.logger = logger
		return nil
	}
}

// WithWebsocketInterval sets the interval between pings for a websocket connection
func WithWebsocketInterval(d time.Duration) Option {
	return func(s *Server) error {
		s.wsPingInterval = d
		return nil
	}
}

// WithPublicAddr sets the public address of the current server
func WithPublicAddr(addr string) Option {
	return func(s *Server) error {
		s.publicAddr = addr
		return nil
	}
}

// Server is an http server on top of the given queue (defaults to a file based queue)
type Server struct {
	middlewares         []func(http.Handler) http.Handler
	handler             http.Handler
	logger              Logger
	metrics             Metrics
	publicAddr          string
	defaultConsumeLimit int64
	consumerGroupLock   *sync.Map
	q                   Queue
	consumerManager     ConsumerManager
	closed              chan struct{}
	waitGroup           *sync.WaitGroup
	wsPingInterval      time.Duration
	wsUpgrader          websocket.Upgrader
}

// NewServer creates a new server with the given options
func NewServer(options ...Option) (*Server, error) {
	s := &Server{
		metrics:             noOpMetrics{},
		logger:              noopLogger{},
		publicAddr:          "localhost",
		defaultConsumeLimit: -1,
		consumerGroupLock:   &sync.Map{},
		closed:              make(chan struct{}),
		waitGroup:           &sync.WaitGroup{},
		wsPingInterval:      time.Second * 60,
		wsUpgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			Error: func(w http.ResponseWriter, r *http.Request, status int, err error) {
				if errors.As(err, &websocket.HandshakeError{}) {
					err = errors.Wrap(headers.ErrInvalidWebsocket, err.Error())
				}
				headers.SetError(w, err)
			},
		},
	}
	options = append(options, WithFileQueue([]string{".haraqa"}, true, 5000))

	for _, option := range options {
		if err := option(s); err != nil {
			return nil, errors.Wrap(err, "invalid option")
		}
	}
	if s.consumerManager == nil {
		s.consumerManager = memconsumer.New()
	}

	rawHandler := http.StripPrefix("/raw/", http.FileServer(http.Dir(s.q.RootDir())))
	s.handler = s.route(rawHandler)

	// iterate over middlewares in reverse order
	for j := len(s.middlewares) - 1; j >= 0; j-- {
		s.handler = s.middlewares[j](s.handler)
	}

	return s, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

func (s *Server) route(raw http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.waitGroup != nil {
			s.waitGroup.Add(1)
			defer s.waitGroup.Done()
		}
		switch {
		case strings.HasPrefix(r.URL.Path, "/topics"):
			if len(r.URL.Path) <= len("/topics/") {
				s.HandleGetAllTopics(w, r)
				return
			}
			switch r.Method {
			case http.MethodGet:
				s.HandleConsume(w, r)
			case http.MethodPost:
				s.HandleProduce(w, r)
			case http.MethodOptions:
				s.HandleOptions(w, r)
			case http.MethodPut:
				s.HandleCreateTopic(w, r)
			case http.MethodDelete:
				s.HandleDeleteTopic(w, r)
			case http.MethodPatch:
				s.HandleModifyTopic(w, r)
			default:
				s.logger.Warnf("%s:%s:%s", r.Method, r.URL.Path, "invalid method")
			}
		case strings.HasPrefix(r.URL.Path, "/raw"):
			raw.ServeHTTP(w, r)
		case strings.HasPrefix(r.URL.Path, "/ws/topics"):
			s.HandleWatchTopics(w, r)
		default:
			s.logger.Warnf("%s:%s:%s", r.Method, r.URL.Path, "invalid url")
			w.WriteHeader(http.StatusNotFound)
			_, err := w.Write([]byte("page not found"))
			if err != nil {
				s.logger.Warnf("%s:%s:%s", r.Method, r.URL.Path, err.Error())
			}
		}
	}
}

// Close closes the server and returns any associated errors
func (s *Server) Close() error {
	s.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.logger.Warnf("%s:%s:%s", r.Method, r.URL.Path, headers.ErrClosed.Error())
		headers.SetError(w, headers.ErrClosed)
	})
	if s.closed != nil {
		close(s.closed)
	}
	if s.waitGroup != nil {
		s.waitGroup.Wait()
	}
	if s.q != nil {
		return s.q.Close()
	}
	return nil
}
