package server

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/haraqa/haraqa/internal/headers"
)

func (s *Server) HandleGetAllTopics() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topics, err := s.q.ListTopics()
		if err != nil {
			headers.SetError(w, err)
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

		info, err := s.q.TruncateTopic(topic, request.Truncate)
		if err != nil {
			headers.SetError(w, err)
			return
		}
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
	}
}

func (s *Server) HandleInspectTopic() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			_ = r.Body.Close()
		}

		topic, err := getTopic(mux.Vars(r))
		if err != nil {
			headers.SetError(w, err)
			return
		}
		info, err := s.q.InspectTopic(topic)
		if err != nil {
			headers.SetError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(info)
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

		err = s.q.Produce(topic, sizes, r.Body)
		if err != nil {
			headers.SetError(w, err)
			return
		}
		s.metrics.ProduceMsgs(len(sizes))
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
		id, err := strconv.ParseInt(vars["id"], 10, 64)
		if err != nil {
			headers.SetError(w, headers.ErrInvalidMessageID)
			return
		}

		var n int64
		limitHeader, ok := r.Header[headers.HeaderLimit]
		if !ok {
			n = s.defaultLimit
		} else {
			n, err = strconv.ParseInt(limitHeader[0], 10, 64)
			if err != nil || n == 0 {
				headers.SetError(w, headers.ErrInvalidHeaderLimit)
				return
			}
		}

		count, err := s.q.Consume(topic, id, n, w)
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
	if topic == "" {
		return "", headers.ErrInvalidTopic
	}
	return strings.ToLower(topic), nil
}
