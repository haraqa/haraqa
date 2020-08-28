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

func TestServer_HandleConsume(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	topic := "consume_topic"
	q := NewMockQueue(ctrl)
	gomock.InOrder(
		q.EXPECT().RootDir().Times(1).Return(""),
		q.EXPECT().Consume(topic, int64(123), int64(-1), gomock.Any()).DoAndReturn(func(topic string, offset, limit int64, w http.ResponseWriter) (int, error) {
			w.WriteHeader(http.StatusPartialContent)
			return 10, nil
		}).Times(1),
		q.EXPECT().Consume(topic, int64(123), int64(-1), gomock.Any()).Return(0, nil).Times(1),
		q.EXPECT().Consume(topic, int64(123), int64(-1), gomock.Any()).Return(0, headers.ErrTopicDoesNotExist).Times(1),
		q.EXPECT().Consume(topic, int64(123), int64(-1), gomock.Any()).Return(0, errors.New("test consume error")).Times(1),
	)
	s, err := NewServer(WithQueue(q))
	if err != nil {
		t.Fatal(err)
	}
	// invalid topic
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/", bytes.NewBuffer([]byte("test body")))
		if err != nil {
			t.Fatal(err)
		}

		s.HandleConsume()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatal(resp.Status)
		}
		err = headers.ReadErrors(resp.Header)
		if err != headers.ErrInvalidTopic {
			t.Fatal(err)
		}
	}

	// valid topic, missing id
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic, bytes.NewBuffer([]byte("test body")))
		if err != nil {
			t.Fatal(err)
		}
		s.ServeHTTP(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatal(resp.Status)
		}
		err = headers.ReadErrors(resp.Header)
		if err != headers.ErrInvalidMessageID {
			t.Fatal(err)
		}
	}

	// valid topic, invalid id
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic+"?id=invalid", bytes.NewBuffer([]byte("test body")))
		if err != nil {
			t.Fatal(err)
		}
		s.ServeHTTP(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatal(resp.Status)
		}
		err = headers.ReadErrors(resp.Header)
		if err != headers.ErrInvalidMessageID {
			t.Fatal(err)
		}
	}

	// valid topic, valid id, invalid limit
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic+"?id=123", bytes.NewBuffer([]byte("test body")))
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set(headers.HeaderLimit, "invalid")
		s.ServeHTTP(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatal(resp.Status)
		}
		err = headers.ReadErrors(resp.Header)
		if err != headers.ErrInvalidHeaderLimit {
			t.Fatal(err)
		}
	}

	// valid topic, valid id, happy path
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic+"?id=123", bytes.NewBuffer([]byte("Hello World")))
		if err != nil {
			t.Fatal(err)
		}
		s.ServeHTTP(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusPartialContent {
			t.Fatal(resp.Status)
		}
		err = headers.ReadErrors(resp.Header)
		if err != nil {
			t.Fatal(err)
		}
	}

	// valid topic, valid id, no content
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic+"?id=123", bytes.NewBuffer([]byte("Hello World")))
		if err != nil {
			t.Fatal(err)
		}

		s.ServeHTTP(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatal(resp.Status)
		}
		err = headers.ReadErrors(resp.Header)
		if err != headers.ErrNoContent {
			t.Fatal(err)
		}
	}

	// valid topic, queue error: topic does not exist
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic+"?id=123", bytes.NewBuffer([]byte("Hello World")))
		if err != nil {
			t.Fatal(err)
		}

		s.ServeHTTP(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusPreconditionFailed {
			t.Fatal(resp.Status)
		}
		err = headers.ReadErrors(resp.Header)
		if err != headers.ErrTopicDoesNotExist {
			t.Fatal(err)
		}
	}

	// valid topic, queue error: unknown error
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic+"?id=123", bytes.NewBuffer([]byte("Hello World")))
		if err != nil {
			t.Fatal(err)
		}

		s.ServeHTTP(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatal(resp.Status)
		}
		err = headers.ReadErrors(resp.Header)
		if err.Error() != "test consume error" {
			t.Fatal(err)
		}
	}
}
