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
)

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
		if r.Body == nil {
			protocol.SetError(w, protocol.ErrInvalidBodyMissing)
			return
		}
		defer r.Body.Close()

		topic, err := protocol.GetTopic(mux.Vars(r))
		if err != nil {
			protocol.SetError(w, err)
			return
		}

		var request protocol.ModifyRequest
		if err = json.NewDecoder(r.Body).Decode(&request); err != nil {
			protocol.SetError(w, protocol.ErrInvalidBodyJSON)
			return
		}

		var info protocol.TopicInfo
		if request.Truncate > 0 {
			qInfo, err := s.q.TruncateTopic(topic, request.Truncate)
			if err != nil {
				protocol.SetError(w, err)
				return
			}
			info.MaxOffset = qInfo.MaxOffset
			info.MinOffset = qInfo.MinOffset
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(&info)
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
		info, err := s.q.InspectTopic(topic)
		if err != nil {
			protocol.SetError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(&protocol.TopicInfo{
			MaxOffset: info.MaxOffset,
			MinOffset: info.MinOffset,
		})
	}
}

func (s *Server) HandleProduce() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Body == nil {
			protocol.SetError(w, protocol.ErrInvalidBodyMissing)
			return
		}
		defer r.Body.Close()

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
		if r.Body != nil {
			r.Body.Close()
		}

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
			protocol.SetError(w, protocol.ErrNoContent)
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
