package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/gorilla/mux"
	"github.com/haraqa/haraqa/internal/headers"
	"github.com/pkg/errors"
)

func TestServer_HandleInspectTopic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	topic := "inspected_topic"
	q := NewMockQueue(ctrl)
	gomock.InOrder(
		q.EXPECT().InspectTopic(topic).Return(&headers.TopicInfo{MinOffset: 123, MaxOffset: 456}, nil).Times(1),
		q.EXPECT().InspectTopic(topic).Return(nil, headers.ErrTopicDoesNotExist).Times(1),
		q.EXPECT().InspectTopic(topic).Return(nil, errors.New("test inspect error")).Times(1),
	)
	s := Server{q: q}

	// invalid topic
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/", bytes.NewBuffer([]byte("test body")))
		if err != nil {
			t.Fatal(err)
		}

		s.HandleInspectTopic()(w, r)
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

	// valid topic, happy path
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic, bytes.NewBuffer([]byte(`test body`)))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic})
		s.HandleInspectTopic()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatal(resp.Status)
		}
		err = headers.ReadErrors(resp.Header)
		if err != nil {
			t.Fatal(err)
		}
		var info headers.TopicInfo
		err = json.NewDecoder(resp.Body).Decode(&info)
		if err != nil {
			t.Fatal(err)
		}
		if info.MinOffset != 123 || info.MaxOffset != 456 {
			t.Fatal(info)
		}
	}

	// valid topic, queue error: topic does not exist
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic, bytes.NewBuffer([]byte(`test body`)))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic})
		s.HandleInspectTopic()(w, r)
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
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic, bytes.NewBuffer([]byte(`test body`)))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic})
		s.HandleInspectTopic()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatal(resp.Status)
		}
		err = headers.ReadErrors(resp.Header)
		if err.Error() != "test inspect error" {
			t.Fatal(err)
		}
	}
}
