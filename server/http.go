package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/haraqa/haraqa/protocol"
	"github.com/haraqa/haraqa/server/queue"
	"github.com/pkg/errors"
)

type Option func(*Server) error

func WithQueue(q queue.Queue) Option {
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
	q            queue.Queue
	defaultLimit int64
	dirs         []string
	metrics      Metrics
}

func NewServer(options ...Option) (*Server, error) {
	s := &Server{
		Router:       mux.NewRouter().SkipClean(true),
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

func (s *Server) HandleGetAllTopics() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topics, err := s.q.ListTopics()
		if err != nil {
			protocol.SetError(w, err)
			return
		}

		var response []byte
		switch r.Header.Get("Accept") {
		case "application/json":
			response, _ = json.Marshal(map[string][]string{
				"topics": topics,
			})
		default:
			response = []byte(strings.Join(topics, ","))
		}

		w.Write(response)
	}
}

func (s *Server) HandleCreateTopic() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body.Close()
		}

		topic, err := protocol.GetTopic(mux.Vars(r))
		if err != nil {
			protocol.SetError(w, err)
			return
		}
		err = s.q.CreateTopic(topic)
		if err != nil {
			protocol.SetError(w, err)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}
}

func (s *Server) HandleModifyTopic() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body.Close()
		}

		topic, err := protocol.GetTopic(mux.Vars(r))
		if err != nil {
			protocol.SetError(w, err)
			return
		}
		_, err = s.q.TruncateTopic(topic, 0)
		if err != nil {
			protocol.SetError(w, err)
			return
		}
	}
}

func (s *Server) HandleDeleteTopic() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body.Close()
		}

		topic, err := protocol.GetTopic(mux.Vars(r))
		if err != nil {
			protocol.SetError(w, err)
			return
		}
		err = s.q.DeleteTopic(topic)
		if err != nil {
			protocol.SetError(w, err)
			return
		}
	}
}

func (s *Server) HandleInspectTopic() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body.Close()
		}

		topic, err := protocol.GetTopic(mux.Vars(r))
		if err != nil {
			protocol.SetError(w, err)
			return
		}
		_, err = s.q.InspectTopic(topic)
		if err != nil {
			protocol.SetError(w, err)
			return
		}
	}
}

func (s *Server) HandleProduce() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		topic, err := protocol.GetTopic(vars)
		if err != nil {
			protocol.SetError(w, err)
			return
		}

		sizes, err := protocol.ReadSizes(r.Header)
		if err != nil {
			protocol.SetError(w, err)
			return
		}

		err = s.q.Produce(topic, sizes, r.Body)
		r.Body.Close()
		if err != nil {
			protocol.SetError(w, err)
			return
		}
		s.metrics.ProduceMsgs(len(sizes))
	}
}

func (s *Server) HandleConsume() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		topic, err := protocol.GetTopic(vars)
		if err != nil {
			protocol.SetError(w, err)
			return
		}
		id, err := strconv.ParseInt(vars["id"], 10, 64)
		if err != nil {
			protocol.SetError(w, protocol.ErrInvalidMessageID)
			return
		}

		var n int64
		limitHeader, ok := r.Header[protocol.HeaderLimit]
		if !ok {
			n = s.defaultLimit
		} else {
			n, err = strconv.ParseInt(limitHeader[0], 10, 64)
			if err != nil || n == 0 {
				protocol.SetError(w, protocol.ErrInvalidHeaderLimit)
				return
			}
		}

		info, err := s.q.Consume(topic, id, n)
		if err != nil {
			protocol.SetError(w, err)
			return
		}
		if closer, ok := info.File.(io.Closer); ok {
			defer closer.Close()
		}
		if !info.Exists {
			protocol.SetError(w, errors.New("no content"))
			return
		}
		wHeader := w.Header()
		wHeader[protocol.HeaderStartTime] = []string{info.StartTime.Format(time.ANSIC)}
		wHeader[protocol.HeaderEndTime] = []string{info.EndTime.Format(time.ANSIC)}
		wHeader[protocol.HeaderFileName] = []string{info.Filename}
		wHeader["Content-Type"] = []string{"application/octet-stream"}
		protocol.SetSizes(info.Sizes, wHeader)
		rangeHeader := "bytes=" + strconv.FormatUint(info.StartAt, 10) + "-" + strconv.FormatUint(info.EndAt, 10)
		wHeader["Range"] = []string{rangeHeader}
		r.Header["Range"] = []string{rangeHeader}

		http.ServeContent(w, r, info.Filename, info.EndTime, info.File)
		s.metrics.ConsumeMsgs(len(info.Sizes))
	}
}
