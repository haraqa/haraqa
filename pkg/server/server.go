package server

import (
	"net/http"

	"github.com/gorilla/mux"
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

// WithDirs sets the directories, in order, for the default file queue to use
func WithDirs(dirs ...string) Option {
	return func(s *Server) error {
		if len(dirs) == 0 {
			return errors.New("at least one directory must be given")
		}
		s.dirs = dirs
		return nil
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
		s.defaultLimit = n
		return nil
	}
}

// WithMiddleware adds the given middleware to the endpoints defined in the http router
func WithMiddleware(middleware ...mux.MiddlewareFunc) Option {
	return func(s *Server) error {
		s.middlewares = append(s.middlewares, middleware...)
		return nil
	}
}

// WithFileCaching causes the default file queue to use caching
func WithFileCaching(cache bool) Option {
	return func(s *Server) error {
		s.qFileCache = cache
		return nil
	}
}

// WithFileEntries sets the number of entries in the default file queue before creating a new one
func WithFileEntries(entries int64) Option {
	return func(s *Server) error {
		if entries < 0 {
			return errors.New("invalid FileEntries, value must not be negative")
		}
		s.qFileEntries = entries
		return nil
	}
}

// Server is an http server on top of the given queue (defaults to a file based queue)
type Server struct {
	*mux.Router
	q            Queue
	qFileCache   bool
	qFileEntries int64
	defaultLimit int64
	dirs         []string
	metrics      Metrics
	isClosed     bool
	middlewares  []mux.MiddlewareFunc
}

// NewServer creates a new server with the given options
func NewServer(options ...Option) (*Server, error) {
	s := &Server{
		Router:       mux.NewRouter(),
		metrics:      noOpMetrics{},
		dirs:         []string{".haraqa"},
		defaultLimit: -1,
		qFileCache:   true,
		qFileEntries: 5000,
	}

	for _, option := range options {
		if err := option(s); err != nil {
			return nil, errors.Wrap(err, "invalid option")
		}
	}

	if s.q == nil {
		// default queue
		var err error
		s.q, err = filequeue.New(s.qFileCache, s.qFileEntries, s.dirs...)
		if err != nil {
			return nil, err
		}
	}

	if len(s.middlewares) > 0 {
		s.Router.Use(s.middlewares...)
	}

	s.Router.PathPrefix("/raw/").Handler(http.StripPrefix("/raw/", http.FileServer(http.Dir(s.q.RootDir()))))
	r := s.Router.PathPrefix("/topics").Subrouter()
	r.Path("").Methods(http.MethodGet).Handler(s.HandleGetAllTopics())
	r.Path("/{topic:.*}").Methods(http.MethodPut).Handler(s.HandleCreateTopic())
	r.Path("/{topic:.*}").Methods(http.MethodPatch).Handler(s.HandleModifyTopic())
	r.Path("/{topic:.*}").Methods(http.MethodDelete).Handler(s.HandleDeleteTopic())
	r.Path("/{topic:.*}").Methods(http.MethodPost).Handler(s.HandleProduce())
	r.Path("/{topic:.*}").Methods(http.MethodGet).Handler(s.HandleConsume())
	r.Path("/{topic:.*}").Methods(http.MethodOptions).Handler(s.HandleOptions())

	return s, nil
}

// Close closes the server and returns any associated errors
func (s *Server) Close() error {
	s.isClosed = true
	return s.q.Close()
}
