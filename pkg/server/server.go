package server

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/haraqa/haraqa/internal/headers"

	"github.com/gorilla/websocket"

	"github.com/haraqa/haraqa/internal/filequeue"
	"github.com/pkg/errors"
)

// Option represents a optional function argument to NewServer
type Option func(*Server) error

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

// Server is an http server on top of the given queue (defaults to a file based queue)
type Server struct {
	middlewares         []func(http.Handler) http.Handler
	handler             http.Handler
	metrics             Metrics
	defaultConsumeLimit int64
	q                   Queue
	closed              chan struct{}
	waitGroup           *sync.WaitGroup
	wsPingInterval      time.Duration
	wsUpgrader          websocket.Upgrader
}

// NewServer creates a new server with the given options
func NewServer(options ...Option) (*Server, error) {
	s := &Server{
		metrics:             noOpMetrics{},
		defaultConsumeLimit: -1,
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
			}
		case strings.HasPrefix(r.URL.Path, "/raw"):
			raw.ServeHTTP(w, r)
		case strings.HasPrefix(r.URL.Path, "/ws/topics"):
			s.HandleWatchTopics(w, r)
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("page not found"))
		}
	}
}

// Close closes the server and returns any associated errors
func (s *Server) Close() error {
	s.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers.SetError(w, headers.ErrClosed)
	})
	close(s.closed)
	if s.waitGroup != nil {
		s.waitGroup.Wait()
	}
	return s.q.Close()
}
