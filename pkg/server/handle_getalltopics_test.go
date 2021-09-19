package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/haraqa/haraqa/internal/headers"
)

func TestServer_HandleGetAllTopics(t *testing.T) {
	topics := []string{"topic_1", "topic_2", "topic_3"}
	t.Run("happy path",
		handleGetAllTopics(http.StatusOK, "", nil, topics, "", func(q *MockQueue) {
			q.EXPECT().ListTopics(nil).Return(topics, nil).Times(1)
		}))
	t.Run("happy path - csv",
		handleGetAllTopics(http.StatusOK, "", nil, topics, "text/csv", func(q *MockQueue) {
			q.EXPECT().ListTopics(nil).Return(topics, nil).Times(1)
		}))
	t.Run("happy path - json",
		handleGetAllTopics(http.StatusOK, "", nil, topics, "application/json", func(q *MockQueue) {
			q.EXPECT().ListTopics(nil).Return(topics, nil).Times(1)
		}))
	t.Run("no matching topics",
		handleGetAllTopics(http.StatusOK, "?regex=^r", nil, []string{}, "application/json", func(q *MockQueue) {
			q.EXPECT().ListTopics(regexp.MustCompile("^r")).Return(nil, nil).Times(1)
		}))
	t.Run("error",
		handleGetAllTopics(http.StatusServiceUnavailable, "", headers.ErrClosed, topics, "application/json", func(q *MockQueue) {
			q.EXPECT().ListTopics(nil).Return(nil, headers.ErrClosed).Times(1)
		}))
}

func handleGetAllTopics(status int, query string, errExpected error, topics []string, contentType string, expect func(q *MockQueue)) func(t *testing.T) {
	return func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// setup mock queue
		q := NewMockQueue(ctrl)
		q.EXPECT().RootDir().Times(1).Return("")
		q.EXPECT().Close().Return(nil).Times(1)
		if expect != nil {
			expect(q)
		}

		// setup server
		s, err := NewServer(WithQueue(q))
		if err != nil {
			t.Error(err)
			return
		}
		defer s.Close()

		// create request
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics"+query, bytes.NewBuffer([]byte("example body")))
		if err != nil {
			t.Error(err)
			return
		}
		if contentType != "" {
			r.Header["Accept"] = []string{contentType}
		}

		// handle
		s.ServeHTTP(w, r)

		// check result
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != status {
			t.Error(resp.Status)
		}
		err = headers.ReadErrors(resp.Header)
		if err != errExpected {
			t.Error(err)
		}
		if err != nil {
			return
		}

		// check body
		switch contentType {
		case "application/json":
			v := make(map[string][]string)
			err = json.NewDecoder(resp.Body).Decode(&v)
			if err != nil {
				t.Error(err)
				return
			}
			if !reflect.DeepEqual(v["topics"], topics) {
				t.Error(v)
				t.Error(v["topics"])
				t.Error(topics)
			}
		case "", "text/plain":
			b, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Error(err)
				return
			}
			if string(b) != strings.Join(topics, ",") {
				t.Error(string(b))
			}
		}
	}
}
