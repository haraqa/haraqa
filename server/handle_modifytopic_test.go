package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/gorilla/mux"
	"github.com/haraqa/haraqa/internal/protocol"
	"github.com/pkg/errors"
)

func TestServer_HandleModifyTopic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	topic := "modified_topic"
	q := NewMockQueue(ctrl)
	gomock.InOrder(
		q.EXPECT().TruncateTopic(topic, int64(123)).Return(&protocol.TopicInfo{MinOffset: 123, MaxOffset: 456}, nil).Times(1),
		q.EXPECT().TruncateTopic(topic, int64(123)).Return(nil, protocol.ErrTopicDoesNotExist).Times(1),
		q.EXPECT().TruncateTopic(topic, int64(123)).Return(nil, errors.New("test modify error")).Times(1),
	)
	s := Server{q: q}

	// nil body
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodPatch, "/topics/"+topic, nil)
		if err != nil {
			t.Fatal(err)
		}

		s.HandleModifyTopic()(w, r)
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
		r, err := http.NewRequest(http.MethodPatch, "/topics/", bytes.NewBuffer([]byte("test body")))
		if err != nil {
			t.Fatal(err)
		}

		s.HandleModifyTopic()(w, r)
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

	// valid topic, invalid json
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodPatch, "/topics/"+topic, bytes.NewBuffer([]byte("test body")))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic})
		s.HandleModifyTopic()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err != protocol.ErrInvalidBodyJSON {
			t.Fatal(err)
		}
	}

	// valid topic, empty json
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodPatch, "/topics/"+topic, bytes.NewBuffer([]byte("{}")))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic})
		s.HandleModifyTopic()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err != nil {
			t.Fatal(err)
		}
	}

	// valid topic, happy path
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodPatch, "/topics/"+topic, bytes.NewBuffer([]byte(`{"truncate":123}`)))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic})
		s.HandleModifyTopic()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err != nil {
			t.Fatal(err)
		}
		var info protocol.TopicInfo
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
		r, err := http.NewRequest(http.MethodPatch, "/topics/"+topic, bytes.NewBuffer([]byte(`{"truncate":123}`)))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic})
		s.HandleModifyTopic()(w, r)
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
		r, err := http.NewRequest(http.MethodPatch, "/topics/"+topic, bytes.NewBuffer([]byte(`{"truncate":123}`)))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic})
		s.HandleModifyTopic()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err.Error() != "test modify error" {
			t.Fatal(err)
		}
	}
}
