package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/gorilla/mux"
	"github.com/haraqa/haraqa/protocol"
	"github.com/pkg/errors"
)

func TestServer_HandleProduce(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	topic := "produce_topic"
	q := NewMockQueue(ctrl)
	gomock.InOrder(
		q.EXPECT().Produce(topic, []int64{5, 6}, gomock.Any()).Return(nil).Times(1),
		q.EXPECT().Produce(topic, []int64{5, 6}, gomock.Any()).Return(protocol.ErrTopicDoesNotExist).Times(1),
		q.EXPECT().Produce(topic, []int64{5, 6}, gomock.Any()).Return(errors.New("test produce error")).Times(1),
	)
	s := Server{q: q, metrics: noOpMetrics{}}

	// nil body
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodPost, "/topics/"+topic, nil)
		if err != nil {
			t.Fatal(err)
		}

		s.HandleProduce()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err != protocol.ErrInvalidBodyMissing {
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

		s.HandleProduce()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err != protocol.ErrInvalidTopic {
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
		r = mux.SetURLVars(r, map[string]string{"topic": topic})
		s.HandleProduce()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err != protocol.ErrInvalidHeaderSizes {
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
		r = mux.SetURLVars(r, map[string]string{"topic": topic})
		r.Header.Set(strings.ToLower(protocol.HeaderSizes), "invalid")
		s.HandleProduce()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err != protocol.ErrInvalidHeaderSizes {
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
		r = mux.SetURLVars(r, map[string]string{"topic": topic})
		r.Header.Add(protocol.HeaderSizes, "5")
		r.Header.Add(protocol.HeaderSizes, "6")

		s.HandleProduce()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
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
		r = mux.SetURLVars(r, map[string]string{"topic": topic})
		r.Header.Add(protocol.HeaderSizes, "5")
		r.Header.Add(protocol.HeaderSizes, "6")

		s.HandleProduce()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusPreconditionFailed {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err != protocol.ErrTopicDoesNotExist {
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
		r = mux.SetURLVars(r, map[string]string{"topic": topic})
		r.Header.Add(protocol.HeaderSizes, "5")
		r.Header.Add(protocol.HeaderSizes, "6")

		s.HandleProduce()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err.Error() != "test produce error" {
			t.Fatal(err)
		}
	}
}
