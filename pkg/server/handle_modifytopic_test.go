package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/haraqa/haraqa/internal/headers"
	"github.com/pkg/errors"
)

func TestServer_HandleModifyTopic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	topic := "modified_topic"
	q := NewMockQueue(ctrl)
	gomock.InOrder(
		q.EXPECT().RootDir().Times(1).Return(""),
		q.EXPECT().ModifyTopic(topic, gomock.Any()).Return(&headers.TopicInfo{MinOffset: 123, MaxOffset: 456}, nil).Times(1),
		q.EXPECT().ModifyTopic(topic, gomock.Any()).Return(nil, headers.ErrTopicDoesNotExist).Times(1),
		q.EXPECT().ModifyTopic(topic, gomock.Any()).Return(nil, errors.New("test modify error")).Times(1),
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
		r, err := http.NewRequest(http.MethodPatch, "/topics/"+topic, nil)
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
		r, err := http.NewRequest(http.MethodPatch, "/topics/", bytes.NewBuffer([]byte("test body")))
		if err != nil {
			t.Fatal(err)
		}

		s.HandleModifyTopic(w, r)
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

	// valid topic, invalid json
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodPatch, "/topics/"+topic, bytes.NewBuffer([]byte("test body")))
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
		if err != headers.ErrInvalidBodyJSON {
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

	// valid topic, happy path
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodPatch, "/topics/"+topic, bytes.NewBuffer([]byte(`{"truncate":123}`)))
		if err != nil {
			t.Fatal(err)
		}
		s.ServeHTTP(w, r)
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
		r, err := http.NewRequest(http.MethodPatch, "/topics/"+topic, bytes.NewBuffer([]byte(`{"truncate":123}`)))
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
		r, err := http.NewRequest(http.MethodPatch, "/topics/"+topic, bytes.NewBuffer([]byte(`{"truncate":123}`)))
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
		if err.Error() != "test modify error" {
			t.Fatal(err)
		}
	}
}
