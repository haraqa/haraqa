package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/haraqa/haraqa/internal/headers"
)

func TestServer_HandleOptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	q := NewMockQueue(ctrl)
	gomock.InOrder(
		q.EXPECT().RootDir().Times(1).Return(""),
		q.EXPECT().Close().Return(nil).Times(1),
	)
	s, err := NewServer(WithQueue(q))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// invalid topic
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodOptions, "/topics/sometopic", bytes.NewBuffer([]byte("test body")))
		if err != nil {
			t.Fatal(err)
		}
		r.Header["Access-Control-Request-Headers"] = []string{"X-Example-Header", "X-test-Header", "   "}
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
		vals := resp.Header.Values("Access-Control-Allow-Headers")
		if len(vals) != 2 {
			t.Fatal(len(vals), vals)
		}
		if vals[0] != "X-Example-Header" {
			t.Error(vals)
		}
		if vals[1] != "X-Test-Header" {
			t.Error(vals)
		}
	}
}
