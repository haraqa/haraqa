package server

import (
	"net/http"
	"strings"

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
func WithFileQueue(dirs []string, cache bool, FileEntries int64) Option {
	return func(s *Server) error {
		if len(dirs) == 0 {
			return errors.New("at least one directory must be given")
		}
		if FileEntries < 0 {
			return errors.New("invalid FileEntries, value must not be negative")
		}
		var err error
		s.q, err = filequeue.New(cache, FileEntries, dirs...)
		return err
	}
}

// WithDirs sets the directories, in order, for the default file queue to use
func WithDirs(dirs ...string) Option {
	return func(s *Server) error {
		if len(dirs) == 0 {
			return errors.New("at least one directory must be given")
		}
		s.qDirs = dirs
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
func WithMiddleware(middleware ...func(http.Handler) http.Handler) Option {
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
	middlewares  []func(http.Handler) http.Handler
	handlers     [8]http.Handler
	metrics      Metrics
	defaultLimit int64
	q            Queue
	qDirs        []string
	qFileCache   bool
	qFileEntries int64
	isClosed     bool
}

// NewServer creates a new server with the given options
func NewServer(options ...Option) (*Server, error) {
	s := &Server{
		metrics:      noOpMetrics{},
		qDirs:        []string{".haraqa"},
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
		s.q, err = filequeue.New(s.qFileCache, s.qFileEntries, s.qDirs...)
		if err != nil {
			return nil, err
		}
	}

	s.handlers = [8]http.Handler{
		s.HandleGetAllTopics(),
		s.HandleConsume(),
		s.HandleProduce(),
		s.HandleOptions(),
		s.HandleCreateTopic(),
		s.HandleDeleteTopic(),
		s.HandleModifyTopic(),
		http.StripPrefix("/raw/", http.FileServer(http.Dir(s.q.RootDir()))),
	}

	// add middlewares
	for i := range s.handlers {
		// iterate over middlewares in reverse order
		for j := len(s.middlewares) - 1; j >= 0; j-- {
			s.handlers[i] = s.middlewares[j](s.handlers[i])
		}
	}

	return s, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/topics"):
		if len(r.URL.Path) <= len("/topics/") {
			s.handlers[0].ServeHTTP(w, r)
			return
		}
		switch r.Method {
		case http.MethodGet:
			s.handlers[1].ServeHTTP(w, r)
		case http.MethodPost:
			s.handlers[2].ServeHTTP(w, r)
		case http.MethodOptions:
			s.handlers[3].ServeHTTP(w, r)
		case http.MethodPut:
			s.handlers[4].ServeHTTP(w, r)
		case http.MethodDelete:
			s.handlers[5].ServeHTTP(w, r)
		case http.MethodPatch:
			s.handlers[6].ServeHTTP(w, r)
		}
	case strings.HasPrefix(r.URL.Path, "/raw"):
		s.handlers[7].ServeHTTP(w, r)
	default:
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("page not found"))
	}
}

// Close closes the server and returns any associated errors
func (s *Server) Close() error {
	s.isClosed = true
	return s.q.Close()
}
