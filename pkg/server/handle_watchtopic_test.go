package server

import (
	"bytes"
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
	"github.com/pkg/errors"
)

func TestServer_HandleWatchTopic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dir := ".haraqa-watch"
	topic := "helloworld"
	mockQ := NewMockQueue(ctrl)
	mockQ.EXPECT().RootDir().Return(dir).AnyTimes()
	mockQ.EXPECT().Close().Return(nil)

	os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, topic), os.ModePerm)
	defer os.RemoveAll(dir)

	s, err := NewServer(WithQueue(mockQ))
	if err != nil {
		t.Error(err)
	}
	s.wsPingInterval = time.Millisecond * 100
	defer s.Close()

	server := httptest.NewServer(s.route(nil))
	server.EnableHTTP2 = true
	defer server.Close()

	// missing topics
	{
		resp, err := http.Get(server.URL + "/ws/topics")
		if err != nil {
			t.Error(err)
		}
		err = headers.ReadErrors(resp.Header)
		if errors.Cause(err) != headers.ErrInvalidTopic {
			t.Error(err)
		}
	}

	// missing topic
	{
		resp, err := http.Get(server.URL + "/ws/topics/invalid_topic")
		if err != nil {
			t.Error(err)
		}
		err = headers.ReadErrors(resp.Header)
		if errors.Cause(err) != headers.ErrTopicDoesNotExist {
			t.Error(err)
		}
	}

	// invalid websocket
	{
		resp, err := http.Post(server.URL+"/ws/topics/"+topic, "application/json", nil)
		if err != nil {
			t.Error(err)
		}
		if resp.StatusCode != http.StatusBadRequest {
			t.Error(resp.StatusCode)
		}
		err = headers.ReadErrors(resp.Header)
		if err == nil {
			t.Error("expected websocket error")
		}
	}

	// valid websocket
	{
		url := strings.Replace(server.URL, "http", "ws", 1) + "/ws/topics/" + topic
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

		go func() {
			f, err := os.Create(filepath.Join(dir, topic, "00000"))
			if err != nil {
				t.Error(err)
				return
			}
			defer f.Close()

			f.Write([]byte("hello_there"))
		}()

		time.Sleep(time.Second)

		msgType, data, err := conn.ReadMessage()
		if err != nil {
			t.Error(err)
			return
		}
		if msgType != websocket.TextMessage {
			t.Error(msgType)
		}
		if !bytes.Equal(data, []byte(topic)) {
			t.Error(string(data))
		}
		conn.Close()
	}
}
