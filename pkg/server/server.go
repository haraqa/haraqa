package server

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/haraqa/haraqa/internal/queue"
	"github.com/pkg/errors"
)

type Option func(*Server) error

func WithQueue(q Queue) Option {
	return func(s *Server) error {
		if q == nil {
			return errors.New("queue cannot be nil")
		}
		s.q = q
		return nil
	}
}

func WithDirs(dirs ...string) Option {
	return func(s *Server) error {
		if len(dirs) == 0 {
			return errors.New("at least one directory must be given")
		}
		s.dirs = dirs
		return nil
	}
}

func WithMetrics(metrics Metrics) Option {
	return func(s *Server) error {
		if metrics == nil {
			return errors.New("metrics cannot be nil")
		}
		s.metrics = metrics
		return nil
	}
}

func WithDefaultConsumeLimit(n int64) Option {
	return func(s *Server) error {
		if n <= 0 {
			n = -1
		}
		s.defaultLimit = n
		return nil
	}
}

type Server struct {
	*mux.Router
	q            Queue
	defaultLimit int64
	dirs         []string
	metrics      Metrics
}

func NewServer(options ...Option) (*Server, error) {
	s := &Server{
		Router:       mux.NewRouter(),
		metrics:      noOpMetrics{},
		dirs:         []string{".haraqa"},
		defaultLimit: -1,
	}

	for _, option := range options {
		if err := option(s); err != nil {
			return nil, errors.Wrap(err, "invalid option")
		}
	}

	if s.q == nil {
		// default queue
		var err error
		s.q, err = queue.NewFileQueue(s.dirs...)
		if err != nil {
			return nil, err
		}
	}

	s.Router.PathPrefix("/raw/").Handler(http.StripPrefix("/raw/", http.FileServer(http.Dir(s.q.RootDir()))))
	r := s.Router.PathPrefix("/topics").Subrouter()
	r.Path("/").Methods(http.MethodGet).Handler(s.HandleGetAllTopics())
	r.Path("/{topic}").Methods(http.MethodPut).Handler(s.HandleCreateTopic())
	r.Path("/{topic}").Methods(http.MethodPatch).Handler(s.HandleModifyTopic())
	r.Path("/{topic}").Methods(http.MethodDelete).Handler(s.HandleDeleteTopic())
	r.Path("/{topic}").Methods(http.MethodGet).Handler(s.HandleInspectTopic())
	r.Path("/{topic}").Methods(http.MethodPost).Handler(s.HandleProduce())
	r.Path("/{topic}/{id}").Methods(http.MethodGet).Handler(s.HandleConsume())

	return s, nil
}

func (s *Server) Close() error {
	return s.q.Close()
}
