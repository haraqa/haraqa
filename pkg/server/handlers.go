package server

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/gorilla/websocket"
	"github.com/pkg/errors"

	"github.com/haraqa/haraqa/internal/headers"
)

// HandleOptions handles requests to the /topics/... endpoints with method == OPTIONS
func (s *Server) HandleOptions(w http.ResponseWriter, r *http.Request) {
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
}

// HandleGetAllTopics handles requests to the /topics endpoints with method == GET.
// It returns all topics currently defined in the queue as either a json or csv depending on the
// request content-type header
func (s *Server) HandleGetAllTopics(w http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		_ = r.Body.Close()
	}

	query := r.URL.Query()
	topics, err := s.q.ListTopics(query.Get("prefix"), query.Get("suffix"), query.Get("regex"))
	if err != nil {
		s.logger.Warnf("%s:%s:list error: %s", r.Method, r.URL.Path, err.Error())
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
	_, err = w.Write(response)
	if err != nil {
		s.logger.Warnf("%s:%s:write error: %s", r.Method, r.URL.Path, err.Error())
	}
}

// HandleCreateTopic handles requests to the /topics/... endpoints with method == PUT.
// It will create a topic if the topic does not exist.
func (s *Server) HandleCreateTopic(w http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		_ = r.Body.Close()
	}

	topic, err := getTopic(r)
	if err != nil {
		s.logger.Warnf("%s:%s:topic error: %s", r.Method, r.URL.Path, err.Error())
		headers.SetError(w, err)
		return
	}
	err = s.q.CreateTopic(topic)
	if err != nil {
		s.logger.Warnf("%s:%s:create topic: %s", r.Method, r.URL.Path, err.Error())
		headers.SetError(w, err)
		return
	}
	w.Header()[headers.ContentType] = []string{"text/plain"}
	w.WriteHeader(http.StatusCreated)
}

// HandleModifyTopic handles requests to the /topics/... endpoints with method == PATCH.
// It will modify the topic if the topic exists. This is used to truncate topics by message
// offset or mod time.
func (s *Server) HandleModifyTopic(w http.ResponseWriter, r *http.Request) {
	if r.Body == nil {
		s.logger.Warnf("%s:%s:body required: %s", r.Method, r.URL.Path, headers.ErrInvalidBodyMissing.Error())
		headers.SetError(w, headers.ErrInvalidBodyMissing)
		return
	}
	defer func() {
		_ = r.Body.Close()
	}()

	topic, err := getTopic(r)
	if err != nil {
		s.logger.Warnf("%s:%s:topic error: %s", r.Method, r.URL.Path, err.Error())
		headers.SetError(w, err)
		return
	}

	addr, err := s.q.GetTopicOwner(topic)
	if err != nil {
		s.logger.Warnf("%s:%s:get topic owner: %s", r.Method, r.URL.Path, err.Error())
		headers.SetError(w, headers.ErrInvalidBodyJSON)
		return
	}
	if addr != "" && addr != s.publicAddr {
		s.handleProxy(w, r, addr)
		return
	}

	var request headers.ModifyRequest
	if err = json.NewDecoder(r.Body).Decode(&request); err != nil {
		s.logger.Warnf("%s:%s:json decode: %s", r.Method, r.URL.Path, err.Error())
		headers.SetError(w, headers.ErrInvalidBodyJSON)
		return
	}

	if request.Truncate == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	info, err := s.q.ModifyTopic(topic, request)
	if err != nil {
		s.logger.Warnf("%s:%s:modify topic: %s", r.Method, r.URL.Path, err.Error())
		headers.SetError(w, err)
		return
	}
	w.Header()[headers.ContentType] = []string{"application/json"}
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(&info)
	if err != nil {
		s.logger.Warnf("%s:%s:json write: %s", r.Method, r.URL.Path, err.Error())
	}
}

// HandleDeleteTopic handles requests to the /topics/... endpoints with method == DELETE.
// It will delete a topic if the topic exists.
func (s *Server) HandleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		_ = r.Body.Close()
	}

	topic, err := getTopic(r)
	if err != nil {
		s.logger.Warnf("%s:%s:topic error: %s", r.Method, r.URL.Path, err.Error())
		headers.SetError(w, err)
		return
	}

	addr, err := s.q.GetTopicOwner(topic)
	if err != nil {
		s.logger.Warnf("%s:%s:get topic owner: %s", r.Method, r.URL.Path, err.Error())
		headers.SetError(w, headers.ErrInvalidBodyJSON)
		return
	}
	if addr != "" && addr != s.publicAddr {
		s.handleProxy(w, r, addr)
		return
	}

	err = s.q.DeleteTopic(topic)
	if err != nil {
		s.logger.Warnf("%s:%s:delete topic: %s", r.Method, r.URL.Path, err.Error())
		headers.SetError(w, err)
		return
	}
	w.Header()[headers.ContentType] = []string{"text/plain"}
	w.WriteHeader(http.StatusNoContent)
}

// HandleProduce handles requests to the /topics/... endpoints with method == POST.
// It will add the given messages to the queue topic
func (s *Server) HandleProduce(w http.ResponseWriter, r *http.Request) {
	if r.Body == nil {
		s.logger.Warnf("%s:%s:body required: %s", r.Method, r.URL.Path, headers.ErrInvalidBodyMissing.Error())
		headers.SetError(w, headers.ErrInvalidBodyMissing)
		return
	}
	defer func() {
		_ = r.Body.Close()
	}()

	topic, err := getTopic(r)
	if err != nil {
		s.logger.Warnf("%s:%s:topic error: %s", r.Method, r.URL.Path, err.Error())
		headers.SetError(w, err)
		return
	}

	addr, err := s.q.GetTopicOwner(topic)
	if err != nil {
		s.logger.Warnf("%s:%s:get topic owner: %s", r.Method, r.URL.Path, err.Error())
		headers.SetError(w, headers.ErrInvalidBodyJSON)
		return
	}
	if addr != "" && addr != s.publicAddr {
		s.handleProxy(w, r, addr)
		return
	}

	sizes, err := headers.ReadSizes(r.Header)
	if err != nil {
		s.logger.Warnf("%s:%s:read sizes: %s", r.Method, r.URL.Path, err.Error())
		headers.SetError(w, err)
		return
	}

	err = s.q.Produce(topic, sizes, uint64(time.Now().UTC().Unix()), r.Body)
	if err != nil {
		s.logger.Warnf("%s:%s:produce: %s", r.Method, r.URL.Path, err.Error())
		headers.SetError(w, err)
		return
	}
	s.metrics.ProduceMsgs(len(sizes))
	w.Header()[headers.ContentType] = []string{"text/plain"}
	w.WriteHeader(http.StatusNoContent)
}

// HandleConsume handles requests to the /topics/... endpoints with method == GET.
// It will retrieve messages from the queue topic
func (s *Server) HandleConsume(w http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		_ = r.Body.Close()
	}

	topic, err := getTopic(r)
	if err != nil {
		s.logger.Warnf("%s:%s:topic error: %s", r.Method, r.URL.Path, err.Error())
		headers.SetError(w, err)
		return
	}

	addr, err := s.q.GetTopicOwner(topic)
	if err != nil {
		s.logger.Warnf("%s:%s:get topic owner: %s", r.Method, r.URL.Path, err.Error())
		headers.SetError(w, headers.ErrInvalidBodyJSON)
		return
	}
	if addr != "" && addr != s.publicAddr {
		s.handleProxy(w, r, addr)
		return
	}

	group := getFirst(r.Header, headers.HeaderConsumerGroup)
	if group != "" {
		tmp, _ := s.consumerGroupLock.LoadOrStore(group+"/"+topic, &sync.Mutex{})
		if lock, ok := tmp.(*sync.Mutex); ok {
			lock.Lock()
			defer lock.Unlock()
		}
	}
	id, err := strconv.ParseInt(getFirst(r.Header, headers.HeaderID), 10, 64)
	if err != nil {
		s.logger.Warnf("%s:%s:parse id: %s", r.Method, r.URL.Path, err.Error())
		headers.SetError(w, headers.ErrInvalidMessageID)
		return
	}

	limit := s.defaultConsumeLimit
	queryLimit := getFirst(r.Header, headers.HeaderLimit)
	if queryLimit != "" && queryLimit[0] != '-' {
		limit, err = strconv.ParseInt(queryLimit, 10, 64)
		if err != nil {
			s.logger.Warnf("%s:%s:parse limit: %s", r.Method, r.URL.Path, err.Error())
			headers.SetError(w, headers.ErrInvalidMessageLimit)
			return
		}
		if limit <= 0 {
			limit = s.defaultConsumeLimit
		}
	}

	count, err := s.q.Consume(group, topic, id, limit, w)
	if err != nil {
		s.logger.Warnf("%s:%s:consume: %s", r.Method, r.URL.Path, err.Error())
		headers.SetError(w, err)
		return
	}
	if count == 0 {
		headers.SetError(w, headers.ErrNoContent)
		return
	}
	s.metrics.ConsumeMsgs(count)
}

func getFirst(m map[string][]string, key string) string {
	v, ok := m[key]
	if !ok || len(v) == 0 {
		return ""
	}
	return v[0]
}

// HandleWatchTopics accepts websocket connections and watches the topic files for writes
func (s *Server) HandleWatchTopics(w http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		_ = r.Body.Close()
	}

	// get topic from url & header
	topics, err := getWatchTopics(r)
	if err != nil {
		s.logger.Warnf("%s:%s:topic error: %s", r.Method, r.URL.Path, err.Error())
		headers.SetError(w, err)
		return
	}

	addrs := map[string]bool{}
	for topic := range topics {
		addr, err := s.q.GetTopicOwner(topic)
		if err != nil {
			s.logger.Warnf("%s:%s:get topic owner: %s", r.Method, r.URL.Path, err.Error())
			headers.SetError(w, headers.ErrInvalidBodyJSON)
			return
		}
		addrs[addr] = true
	}
	if len(addrs) > 1 {
		// TODO: enable proxy to multiple
		s.logger.Warnf("%s:%s:watch topics: %s", r.Method, r.URL.Path, "cannot proxy to multiple servers")
		headers.SetError(w, headers.ErrProxyFailed)
		return
	}

	if !addrs[s.publicAddr] && !addrs[""] {
		for addr := range addrs {
			s.handleProxy(w, r, addr)
			return
		}
	}

	// setup watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		s.logger.Warnf("%s:%s:new watcher: %s", r.Method, r.URL.Path, err.Error())
		headers.SetError(w, err)
		return
	}
	defer watcher.Close()
	rootDir := s.q.RootDir()
	for topic := range topics {
		err = watcher.Add(rootDir + string(filepath.Separator) + topic)
		if err != nil {
			s.logger.Warnf("%s:%s:watcher add: %s", r.Method, r.URL.Path, err.Error())
			if os.IsNotExist(err) {
				err = headers.ErrTopicDoesNotExist
			}

			headers.SetError(w, err)
			return
		}
	}

	// upgrade request to websocket connection
	conn, err := s.wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Warnf("%s:%s:websocket upgrade: %s", r.Method, r.URL.Path, err.Error())
		return
	}
	defer conn.Close()

	// add ping/pong handler timers
	pingT := time.NewTicker(s.wsPingInterval)
	defer pingT.Stop()
	conn.SetPongHandler(func(appData string) error {
		err := conn.SetReadDeadline(time.Now().Add(2 * s.wsPingInterval))
		if err != nil {
			s.logger.Warnf("%s:%s:set ws deadline: %s", r.Method, r.URL.Path, err.Error())
		}
		return err
	})

	// add a reader loop to handle ping/pong/close
	wsClosed := make(chan error, 1)
	go readNoopWebsocket(conn, wsClosed)

	// send initial ping
	if err = conn.WriteMessage(websocket.PingMessage, nil); err != nil {
		s.logger.Warnf("%s:%s:ping: %s", r.Method, r.URL.Path, err.Error())
		return
	}
	if err = conn.SetReadDeadline(time.Now().Add(2 * s.wsPingInterval)); err != nil {
		s.logger.Warnf("%s:%s:set initial ws deadline: %s", r.Method, r.URL.Path, err.Error())
		return
	}

	// loop waiting for an event or timeout
	for {
		select {
		case event := <-watcher.Events:
			if event.Op == fsnotify.Write && !strings.HasSuffix(event.Name, ".log") {
				topic := strings.TrimPrefix(filepath.Dir(event.Name), rootDir+string(filepath.Separator))
				err = conn.WriteMessage(websocket.TextMessage, []byte(topic))
				err = errors.Wrap(err, "cannot write topic")
			} else if event.Op == fsnotify.Remove {
				topic := strings.TrimPrefix(filepath.Dir(event.Name), rootDir+string(filepath.Separator))
				err = watcher.Remove(filepath.Dir(event.Name))
				err = errors.Wrap(err, "cannot remove watcher item")
				delete(topics, topic)
				if len(topics) == 0 {
					s.logger.Warnf("%s:%s:deleted all topics: %s", r.Method, r.URL.Path, "all topics removed, closing ws connection")
					msg := websocket.FormatCloseMessage(websocket.CloseGoingAway, headers.ErrTopicDoesNotExist.Error())
					_ = conn.WriteControl(websocket.CloseGoingAway, msg, time.Now().Add(s.wsPingInterval))
					return
				}
			}
		case <-pingT.C:
			err = conn.WriteMessage(websocket.PingMessage, []byte{})
			err = errors.Wrap(err, "cannot write ping")
		case err = <-wsClosed:
			if codeErr, ok := err.(*websocket.CloseError); ok && codeErr.Code == websocket.CloseNormalClosure {
				return
			}
			err = errors.Wrap(err, "closed")
		case <-s.closed:
			s.logger.Warnf("%s:%s:closing server: %s", r.Method, r.URL.Path, "server closing, closing ws connection")
			msg := websocket.FormatCloseMessage(websocket.CloseGoingAway, headers.ErrClosed.Error())
			_ = conn.WriteControl(websocket.CloseGoingAway, msg, time.Now().Add(s.wsPingInterval))
			return
		}
		if err != nil {
			s.logger.Warnf("%s:%s:%s", r.Method, r.URL.Path, err.Error())
			return
		}
	}
}

func getTopic(r *http.Request) (string, error) {
	i := strings.Index(strings.ToLower(r.URL.Path), "/topics/")
	if i < 0 {
		return "", headers.ErrInvalidTopic
	}
	topic := strings.ToLower(filepath.Clean(r.URL.Path[i+len("/topics/"):]))
	if topic == "" || topic == "." {
		return "", headers.ErrInvalidTopic
	}
	return topic, nil
}

func getWatchTopics(r *http.Request) (map[string]bool, error) {
	topics := make(map[string]bool)
	for _, topic := range r.Header.Values(headers.HeaderWatchTopics) {
		topics[strings.ToLower(filepath.Clean(topic))] = true
	}
	topic, err := getTopic(r)
	if err == nil {
		topics[topic] = true
	}
	if len(topics) == 0 {
		return nil, headers.ErrInvalidTopic
	}
	return topics, nil
}

func readNoopWebsocket(conn *websocket.Conn, ch chan error) {
	for {
		// continuously read until an error occurs
		if _, _, err := conn.NextReader(); err != nil {
			ch <- err
			return
		}
	}
}
