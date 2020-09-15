package server

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/haraqa/haraqa/internal/headers"
)

func (s *Server) HandleOptions() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			_ = r.Body.Close()
		}
		requestHeaders := r.Header.Values("Access-Control-Request-Headers")
		allowedHeaders := make([]string, 0, len(requestHeaders))
		for _, v := range requestHeaders {
			canonicalHeader := http.CanonicalHeaderKey(strings.TrimSpace(v))
			if canonicalHeader == "" {
				continue
			}
			allowedHeaders = append(allowedHeaders, canonicalHeader)
		}
		if len(allowedHeaders) > 0 {
			w.Header()["Access-Control-Allow-Headers"] = allowedHeaders
		}
		method := r.Header.Get("Access-Control-Request-Method")
		w.Header().Set("Access-Control-Allow-Methods", method)
		w.WriteHeader(http.StatusOK)
		return
	}
}

func (s *Server) HandleGetAllTopics() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topics, err := s.q.ListTopics()
		if err != nil {
			headers.SetError(w, err)
			return
		}
		if topics == nil {
			topics = []string{}
		}

		var response []byte
		switch r.Header.Get("Accept") {
		case "application/json":
			w.Header()[headers.ContentType] = []string{"application/json"}
			response, _ = json.Marshal(map[string][]string{
				"topics": topics,
			})
		default:
			w.Header()[headers.ContentType] = []string{"text/csv"}
			response = []byte(strings.Join(topics, ","))
		}
		_, _ = w.Write(response)
	}
}

func (s *Server) HandleCreateTopic() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			_ = r.Body.Close()
		}

		topic, err := getTopic(mux.Vars(r))
		if err != nil {
			headers.SetError(w, err)
			return
		}
		err = s.q.CreateTopic(topic)
		if err != nil {
			headers.SetError(w, err)
			return
		}
		w.Header()[headers.ContentType] = []string{"text/plain"}
		w.WriteHeader(http.StatusCreated)
	}
}

func (s *Server) HandleModifyTopic() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Body == nil {
			headers.SetError(w, headers.ErrInvalidBodyMissing)
			return
		}
		defer func() {
			_ = r.Body.Close()
		}()

		topic, err := getTopic(mux.Vars(r))
		if err != nil {
			headers.SetError(w, err)
			return
		}

		var request headers.ModifyRequest
		if err = json.NewDecoder(r.Body).Decode(&request); err != nil {
			headers.SetError(w, headers.ErrInvalidBodyJSON)
			return
		}

		if request.Truncate == 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		info, err := s.q.ModifyTopic(topic, request)
		if err != nil {
			headers.SetError(w, err)
			return
		}
		w.Header()[headers.ContentType] = []string{"application/json"}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(&info)
	}
}

func (s *Server) HandleDeleteTopic() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			_ = r.Body.Close()
		}

		topic, err := getTopic(mux.Vars(r))
		if err != nil {
			headers.SetError(w, err)
			return
		}
		err = s.q.DeleteTopic(topic)
		if err != nil {
			headers.SetError(w, err)
			return
		}
		w.Header()[headers.ContentType] = []string{"text/plain"}
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *Server) HandleProduce() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Body == nil {
			headers.SetError(w, headers.ErrInvalidBodyMissing)
			return
		}
		defer func() {
			_ = r.Body.Close()
		}()

		vars := mux.Vars(r)
		topic, err := getTopic(vars)
		if err != nil {
			headers.SetError(w, err)
			return
		}

		sizes, err := headers.ReadSizes(r.Header)
		if err != nil {
			headers.SetError(w, err)
			return
		}

		err = s.q.Produce(topic, sizes, uint64(time.Now().Unix()), r.Body)
		if err != nil {
			headers.SetError(w, err)
			return
		}
		s.metrics.ProduceMsgs(len(sizes))
		w.Header()[headers.ContentType] = []string{"text/plain"}
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *Server) HandleConsume() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			_ = r.Body.Close()
		}

		vars := mux.Vars(r)
		topic, err := getTopic(vars)
		if err != nil {
			headers.SetError(w, err)
			return
		}

		id, err := strconv.ParseInt(r.URL.Query().Get("id"), 10, 64)
		if err != nil {
			headers.SetError(w, headers.ErrInvalidMessageID)
			return
		}

		limit := s.defaultLimit
		queryLimit := r.URL.Query().Get("limit")
		if queryLimit != "" && queryLimit[0] != '-' {
			limit, err = strconv.ParseInt(queryLimit, 10, 64)
			if err != nil {
				headers.SetError(w, headers.ErrInvalidMessageLimit)
				return
			}
			if limit <= 0 {
				limit = s.defaultLimit
			}
		}

		count, err := s.q.Consume(topic, id, limit, w)
		if err != nil {
			headers.SetError(w, err)
			return
		}
		if count == 0 {
			headers.SetError(w, headers.ErrNoContent)
			return
		}
		s.metrics.ConsumeMsgs(count)
	}
}

func getTopic(vars map[string]string) (string, error) {
	topic, _ := vars["topic"]
	topic = filepath.Clean(strings.ToLower(topic))
	if topic == "" || topic == "." {
		return "", headers.ErrInvalidTopic
	}
	return topic, nil
}
