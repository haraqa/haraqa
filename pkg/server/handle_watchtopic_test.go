package server

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/gorilla/websocket"

	"github.com/haraqa/haraqa/internal/headers"
)

func TestServer_HandleWatchTopic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dir := ".haraqa-watch"
	topic := "helloworld"
	mockQ := NewMockQueue(ctrl)
	mockQ.EXPECT().RootDir().Return(dir).AnyTimes()
	mockQ.EXPECT().Close().Return(nil)
	mockQ.EXPECT().GetTopicOwner(gomock.Any()).Return("", nil).AnyTimes()

	os.RemoveAll(dir)
	os.MkdirAll(dir+string(filepath.Separator)+topic, os.ModePerm)
	defer os.RemoveAll(dir)

	s, err := NewServer(WithQueue(mockQ), WithLogger(TestLogger{t}))
	if err != nil {
		t.Error(err)
	}
	s.wsPingInterval = time.Millisecond * 100
	defer s.Close()

	server := httptest.NewServer(s.route())
	server.EnableHTTP2 = true
	defer server.Close()

	t.Run("missing topics", handleWatchTopicErrors(http.StatusBadRequest, server.URL+"/ws/topics", headers.ErrInvalidTopic))
	t.Run("invalid websocket", handleWatchTopicErrors(http.StatusBadRequest, server.URL+"/ws/topics/"+topic, headers.ErrInvalidWebsocket))

	// valid websocket
	{
		url := strings.Replace(server.URL, "http", "ws", 1) + "/ws/topics/" + topic
		written := make(chan string, 1)
		deleted := make(chan string, 1)
		mockQ.EXPECT().WatchTopics(gomock.Any()).AnyTimes().Return(written, deleted, io.NopCloser(nil), nil)

		conn, resp, err := websocket.DefaultDialer.Dial(url, map[string][]string{
			headers.HeaderWatchTopics: {topic, topic, topic},
		})
		if err != nil {
			t.Error(err)
		}
		err = headers.ReadErrors(resp.Header)
		if err != nil {
			t.Error(err)
		}
		if conn == nil {
			t.Error("invalid conn")
		}
		if resp.Body != nil {
			defer resp.Body.Close()
		}

		err = conn.WriteControl(websocket.PongMessage, nil, time.Now().Add(s.wsPingInterval))
		if err != nil {
			t.Error(err)
			return
		}

		written <- topic
		time.Sleep(s.wsPingInterval * 2)

		msgType, data, err := conn.ReadMessage()
		if err != nil {
			t.Error(err)
			return
		}
		if msgType != websocket.TextMessage {
			t.Error(msgType)
		}
		if !bytes.Equal(data, []byte(topic)) {
			t.Error(string(data), topic)
		}
		conn.Close()
	}
}

func handleWatchTopicErrors(status int, url string, expectedError error) func(*testing.T) {
	return func(t *testing.T) {
		resp, err := http.Post(url, "application/json", nil)
		if err != nil {
			t.Error(err)
		}
		if resp.StatusCode != status {
			t.Error(resp.StatusCode, status)
		}
		err = headers.ReadErrors(resp.Header)
		if err != expectedError {
			t.Error(err, expectedError)
		}
	}
}
