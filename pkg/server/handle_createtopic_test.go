package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/haraqa/haraqa/internal/headers"
	"github.com/pkg/errors"
)

func TestServer_HandleCreateTopic(t *testing.T) {
	topic := "created_topic"
	t.Run("invalid topic",
		handleCreateTopic(http.StatusBadRequest, headers.ErrInvalidTopic, "", nil))
	t.Run("happy path",
		handleCreateTopic(http.StatusCreated, nil, topic, func(q *MockQueue) {
			q.EXPECT().CreateTopic(topic).Return(nil).Times(1)
		}))
	t.Run("topic already exists",
		handleCreateTopic(http.StatusPreconditionFailed, headers.ErrTopicAlreadyExists, topic, func(q *MockQueue) {
			q.EXPECT().CreateTopic(topic).Return(headers.ErrTopicAlreadyExists).Times(1)
		}))
	errUnknown := errors.New("test create error")
	t.Run("unknown error",
		handleCreateTopic(http.StatusInternalServerError, errUnknown, topic, func(q *MockQueue) {
			q.EXPECT().CreateTopic(topic).Return(errUnknown).Times(1)
		}))
}

func handleCreateTopic(status int, errExpected error, topic string, expect func(q *MockQueue)) func(t *testing.T) {
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
		r, err := http.NewRequest(http.MethodPut, "/topics/"+topic, bytes.NewBuffer([]byte("test body")))
		if err != nil {
			t.Error(err)
			return
		}

		// handle
		_, err = getTopic(r)
		if err != nil {
			s.HandleCreateTopic(w, r)
		} else {
			s.ServeHTTP(w, r)
		}

		// check result
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != status {
			t.Error(resp.Status)
		}
		err = headers.ReadErrors(resp.Header)
		if err != errExpected && err.Error() != errExpected.Error() {
			t.Error(err)
		}
	}
}
