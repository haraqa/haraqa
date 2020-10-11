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
	query := r.URL.Query()
	topics, err := s.q.ListTopics(query.Get("prefix"), query.Get("suffix"), query.Get("regex"))
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

// HandleCreateTopic handles requests to the /topics/... endpoints with method == PUT.
// It will create a topic if the topic does not exist.
func (s *Server) HandleCreateTopic(w http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		_ = r.Body.Close()
	}

	topic, err := getTopic(r)
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

// HandleModifyTopic handles requests to the /topics/... endpoints with method == PATCH.
// It will modify the topic if the topic exists. This is used to truncate topics by message
// offset or mod time.
func (s *Server) HandleModifyTopic(w http.ResponseWriter, r *http.Request) {
	if r.Body == nil {
		headers.SetError(w, headers.ErrInvalidBodyMissing)
		return
	}
	defer func() {
		_ = r.Body.Close()
	}()

	topic, err := getTopic(r)
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

// HandleDeleteTopic handles requests to the /topics/... endpoints with method == DELETE.
// It will delete a topic if the topic exists.
func (s *Server) HandleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		_ = r.Body.Close()
	}

	topic, err := getTopic(r)
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

// HandleProduce handles requests to the /topics/... endpoints with method == POST.
// It will add the given messages to the queue topic
func (s *Server) HandleProduce(w http.ResponseWriter, r *http.Request) {
	if r.Body == nil {
		headers.SetError(w, headers.ErrInvalidBodyMissing)
		return
	}
	defer func() {
		_ = r.Body.Close()
	}()

	topic, err := getTopic(r)
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

// HandleConsume handles requests to the /topics/... endpoints with method == GET.
// It will retrieve messages from the queue topic
func (s *Server) HandleConsume(w http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		_ = r.Body.Close()
	}

	topic, err := getTopic(r)
	if err != nil {
		headers.SetError(w, err)
		return
	}

	group := r.Header.Get(headers.HeaderConsumerGroup)
	if group != "" {
		tmp, _ := s.consumerGroupLock.LoadOrStore(group+"/"+topic, &sync.Mutex{})
		if lock, ok := tmp.(*sync.Mutex); ok {
			lock.Lock()
			defer lock.Unlock()
		}
	}

	id, err := strconv.ParseInt(r.URL.Query().Get("id"), 10, 64)
	if err != nil {
		headers.SetError(w, headers.ErrInvalidMessageID)
		return
	}

	limit := s.defaultConsumeLimit
	queryLimit := r.URL.Query().Get("limit")
	if queryLimit != "" && queryLimit[0] != '-' {
		limit, err = strconv.ParseInt(queryLimit, 10, 64)
		if err != nil {
			headers.SetError(w, headers.ErrInvalidMessageLimit)
			return
		}
		if limit <= 0 {
			limit = s.defaultConsumeLimit
		}
	}

	count, err := s.q.Consume(group, topic, id, limit, w)
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

// HandleWatchTopics accepts websocket connections and watches the topic files for writes
func (s *Server) HandleWatchTopics(w http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		_ = r.Body.Close()
	}

	// get topic from url & header
	topics, err := getWatchTopics(r)
	if err != nil {
		headers.SetError(w, err)
		return
	}

	// setup watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		headers.SetError(w, err)
		return
	}
	defer watcher.Close()
	rootDir := s.q.RootDir()
	for topic := range topics {
		err = watcher.Add(filepath.Join(rootDir, topic))
		if err != nil {
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
		return
	}
	defer conn.Close()

	// add ping/pong handler timers
	pingT := time.NewTicker(s.wsPingInterval)
	defer pingT.Stop()
	conn.SetPongHandler(func(appData string) error { return conn.SetReadDeadline(time.Now().Add(2 * s.wsPingInterval)) })

	// add a reader loop to handle ping/pong/close
	wsClosed := make(chan error, 1)
	go readNoopWebsocket(conn, wsClosed)

	// send initial ping
	if err = conn.WriteMessage(websocket.PingMessage, nil); err != nil {
		return
	}
	if err = conn.SetReadDeadline(time.Now().Add(2 * s.wsPingInterval)); err != nil {
		return
	}

	// loop waiting for an event or timeout
	for {
		select {
		case event := <-watcher.Events:
			if event.Op == fsnotify.Write && !strings.HasSuffix(event.Name, ".log") {
				topic := strings.TrimPrefix(filepath.Dir(event.Name), rootDir+string(filepath.Separator))
				err = conn.WriteMessage(websocket.TextMessage, []byte(topic))
			} else if event.Op == fsnotify.Remove {
				topic := strings.TrimPrefix(filepath.Dir(event.Name), rootDir+string(filepath.Separator))
				err = watcher.Remove(filepath.Dir(event.Name))
				delete(topics, topic)
				if len(topics) == 0 {
					_ = conn.WriteControl(websocket.CloseGoingAway, nil, time.Now().Add(s.wsPingInterval))
					return
				}
			}
		case <-pingT.C:
			err = conn.WriteMessage(websocket.PingMessage, []byte{})
		case err = <-wsClosed:
		case <-s.closed:
			_ = conn.WriteControl(websocket.CloseGoingAway, nil, time.Now().Add(s.wsPingInterval))
			return
		}
		if err != nil {
			return
		}
	}
}

func getTopic(r *http.Request) (string, error) {
	split := strings.SplitN(strings.ToLower(r.URL.Path), "/topics/", 2)
	if len(split) < 2 {
		return "", headers.ErrInvalidTopic
	}
	topic := strings.ToLower(filepath.Clean(split[1]))
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
