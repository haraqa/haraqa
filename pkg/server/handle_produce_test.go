package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/haraqa/haraqa/internal/headers"
	"github.com/pkg/errors"
)

func TestServer_HandleProduce(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	topic := "produce_topic"
	q := NewMockQueue(ctrl)
	gomock.InOrder(
		q.EXPECT().RootDir().Times(1).Return(""),
		q.EXPECT().Produce(topic, []int64{5, 6}, gomock.Any(), gomock.Any()).Return(nil).Times(1),
		q.EXPECT().Produce(topic, []int64{5, 6}, gomock.Any(), gomock.Any()).Return(headers.ErrTopicDoesNotExist).Times(1),
		q.EXPECT().Produce(topic, []int64{5, 6}, gomock.Any(), gomock.Any()).Return(errors.New("test produce error")).Times(1),
		q.EXPECT().Close().Return(nil).Times(1),
	)
	s, err := NewServer(WithQueue(q))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// nil body
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodPost, "/topics/"+topic, nil)
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
		if err != headers.ErrInvalidBodyMissing {
			t.Fatal(err)
		}
	}

	// invalid topic
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodPost, "/topics/", bytes.NewBuffer([]byte("test body")))
		if err != nil {
			t.Fatal(err)
		}

		s.HandleProduce(w, r)
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

	// valid topic, missing read sizes
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodPost, "/topics/"+topic, bytes.NewBuffer([]byte("test body")))
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
		if err != headers.ErrInvalidHeaderSizes {
			t.Fatal(err)
		}
	}

	// valid topic, invalid read sizes
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodPost, "/topics/"+topic, bytes.NewBuffer([]byte("test body")))
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set(strings.ToLower(headers.HeaderSizes), "invalid")
		s.ServeHTTP(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatal(resp.Status)
		}
		err = headers.ReadErrors(resp.Header)
		if err != headers.ErrInvalidHeaderSizes {
			t.Fatal(err)
		}
	}

	// valid topic, valid sizes
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodPost, "/topics/"+topic, bytes.NewBuffer([]byte("Hello World")))
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Add(headers.HeaderSizes, "5")
		r.Header.Add(headers.HeaderSizes, "6")

		s.ServeHTTP(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatal(resp.Status)
		}
		err = headers.ReadErrors(resp.Header)
		if err != nil {
			t.Fatal(err)
		}
	}

	// valid topic, queue error: topic does not exist
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodPost, "/topics/"+topic, bytes.NewBuffer([]byte("Hello World")))
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Add(headers.HeaderSizes, "5")
		r.Header.Add(headers.HeaderSizes, "6")

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
		r, err := http.NewRequest(http.MethodPost, "/topics/"+topic, bytes.NewBuffer([]byte("Hello World")))
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Add(headers.HeaderSizes, "5")
		r.Header.Add(headers.HeaderSizes, "6")

		s.ServeHTTP(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatal(resp.Status)
		}
		err = headers.ReadErrors(resp.Header)
		if err.Error() != "test produce error" {
			t.Fatal(err)
		}
	}
}
