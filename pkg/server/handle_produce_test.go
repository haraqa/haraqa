package server

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/haraqa/haraqa/internal/headers"
)

func TestServer_HandleProduce(t *testing.T) {
	topic := "produce_topic"
	t.Run("nil body",
		handleProduce(http.StatusBadRequest, headers.ErrInvalidBodyMissing, "", nil, nil, nil))
	t.Run("invalid topic",
		handleProduce(http.StatusBadRequest, headers.ErrInvalidTopic, "", nil, bytes.NewBuffer([]byte("hello world")), nil))
	t.Run("missing sizes",
		handleProduce(http.StatusBadRequest, headers.ErrInvalidHeaderSizes, topic, nil, bytes.NewBuffer([]byte("hello world")), nil))
	t.Run("invalid sizes",
		handleProduce(http.StatusBadRequest, headers.ErrInvalidHeaderSizes, topic, []string{"invalid"}, bytes.NewBuffer([]byte("hello world")), nil))
	t.Run("valid sizes",
		handleProduce(http.StatusNoContent, nil, topic, []string{"5", "6"}, bytes.NewBuffer([]byte("hello world")), func(q *MockQueue) {
			q.EXPECT().Produce(topic, []int64{5, 6}, gomock.Any(), gomock.Any()).Return(nil).Times(1)
		}))
	t.Run("no such topic",
		handleProduce(http.StatusPreconditionFailed, headers.ErrTopicDoesNotExist, topic, []string{"5", "6"}, bytes.NewBuffer([]byte("hello world")), func(q *MockQueue) {
			q.EXPECT().Produce(topic, []int64{5, 6}, gomock.Any(), gomock.Any()).Return(headers.ErrTopicDoesNotExist).Times(1)
		}))
}

func handleProduce(status int, errExpected error, topic string, sizes []string, body io.Reader, expect func(q *MockQueue)) func(*testing.T) {
	return func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// setup queue
		q := NewMockQueue(ctrl)
		q.EXPECT().Close().Times(1).Return(nil)
		dist := NewMockDistributor(ctrl)
		if expect != nil {
			expect(q)
		}

		// setup server
		s, err := NewServer(WithQueue(q), WithDistributor(dist))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		// make request/response
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodPost, "/topics/"+topic, body)
		if err != nil {
			t.Error(err)
			return
		}
		r.Header.Set(headers.HeaderSizes, strings.Join(sizes, ":"))

		// if no topic, handle directly
		_, err = getTopic(r)
		if err != nil {
			s.HandleProduce(w, r)
		} else {
			dist.EXPECT().GetTopicOwner(topic).Return("", nil)
			s.ServeHTTP(w, r)
		}

		// check results
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != status {
			t.Error(resp.Status, status)
		}
		err = headers.ReadErrors(resp.Header)
		if err != errExpected && err.Error() != errExpected.Error() {
			t.Error(err)
		}
	}
}
