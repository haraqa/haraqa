package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
)

func TestNewServer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// option w/error
	{
		_, err := NewServer(func(server *Server) error {
			return errors.New("test error")
		})
		if err.Error() != "invalid option: test error" {
			t.Fatal(err)
		}
	}

	// with valid option
	{
		q := NewMockQueue(ctrl)
		gomock.InOrder(
			q.EXPECT().RootDir().Return("./.haraqa").Times(1),
			q.EXPECT().Close().Times(1),
		)
		s, err := NewServer(WithQueue(q))
		if err != nil {
			t.Fatal(err)
		}
		s.Close()
	}

	// with invalid dir
	{
		_, err := NewServer(WithFileQueue([]string{"invalid/folder/doesnt/exist/..."}, true, 5000))
		if _, ok := errors.Cause(err).(*os.PathError); !ok {
			t.Fatal(errors.Cause(err))
		}
	}

	// with middleware
	{
		q := NewMockQueue(ctrl)
		gomock.InOrder(
			q.EXPECT().RootDir().Return("./.haraqa").Times(1),
			q.EXPECT().Close().Times(1),
		)
		mw := func(next http.Handler) http.Handler {
			return next
		}
		s, err := NewServer(WithQueue(q), WithMiddleware(mw))
		if err != nil {
			t.Fatal(err)
		}
		if len(s.middlewares) != 1 && !reflect.DeepEqual(s.middlewares[0], mw) {
			t.Error(s.middlewares)
		}
		s.Close()
	}
}

func TestServer_ServeHTTP(t *testing.T) {
	s := &Server{}
	var count int
	for i := range s.handlers {
		v := i
		s.handlers[i] = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if count != v {
				t.Fatal(count, v)
			}
			count++
		})
	}

	w := httptest.NewRecorder()
	r, _ := http.NewRequest(http.MethodGet, "/topics", nil)
	s.ServeHTTP(w, r)
	r, _ = http.NewRequest(http.MethodGet, "/topics/tmp", nil)
	s.ServeHTTP(w, r)
	r, _ = http.NewRequest(http.MethodPost, "/topics/tmp", nil)
	s.ServeHTTP(w, r)
	r, _ = http.NewRequest(http.MethodOptions, "/topics/tmp", nil)
	s.ServeHTTP(w, r)
	r, _ = http.NewRequest(http.MethodPut, "/topics/tmp", nil)
	s.ServeHTTP(w, r)
	r, _ = http.NewRequest(http.MethodDelete, "/topics/tmp", nil)
	s.ServeHTTP(w, r)
	r, _ = http.NewRequest(http.MethodPatch, "/topics/tmp", nil)
	s.ServeHTTP(w, r)
	r, _ = http.NewRequest(http.MethodGet, "/raw/", nil)
	s.ServeHTTP(w, r)
	r, _ = http.NewRequest(http.MethodGet, "/invalid", nil)
	s.ServeHTTP(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatal(w.Code)
	}
}

/*
	// verify routes
	{
		q := NewMockQueue(ctrl)
		q.EXPECT().RootDir().Return(".haraqa").Times(1)
		s, err := NewServer(WithQueue(q))
		if err != nil {
			t.Fatal(err)
		}

		// get all topics
		testRoute(t, s, http.MethodGet, "/topics/", s.HandleGetAllTopics(), "", "")

		// create topic
		testRoute(t, s, http.MethodPut, "/topics/created_topic", s.HandleCreateTopic(), "created_topic", "")

		// delete topic
		testRoute(t, s, http.MethodDelete, "/topics/deleted_topic", s.HandleDeleteTopic(), "deleted_topic", "")

		// modify topic
		testRoute(t, s, http.MethodPatch, "/topics/modified_topic", s.HandleModifyTopic(), "modified_topic", "")

		// inspect topic
		//testRoute(t, s, http.MethodGet, "/topics/inspected_topic", s.HandleInspectTopic(), "inspected_topic", "")

		// produce to topic
		testRoute(t, s, http.MethodPost, "/topics/produce_topic", s.HandleProduce(), "produce_topic", "")

		// consume from topic
		testRoute(t, s, http.MethodGet, "/topics/consume_topic?id=1234", s.HandleConsume(), "consume_topic", "1234")
	}

	// test defaults
	{
		defer func() {
			err := os.Remove(".haraqa")
			if err != nil {
				t.Fatal(err)
			}
		}()
		s, err := NewServer()
		if err != nil {
			t.Error(err)
			return
		}
		defer s.Close()
		if s.defaultLimit != -1 {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(s.dirs, []string{".haraqa"}) {
			t.Error(err)
			return
		}
		if reflect.TypeOf(s.metrics) != reflect.TypeOf(noOpMetrics{}) {
			t.Error(reflect.TypeOf(s.metrics), reflect.TypeOf(noOpMetrics{}))
			return
		}
		if reflect.TypeOf(s.q) != reflect.TypeOf(&filequeue.FileQueue{}) {
			t.Error(reflect.TypeOf(s.q), reflect.TypeOf(&filequeue.FileQueue{}))
			return
		}
		if s.q.RootDir() != ".haraqa" {
			t.Error(s.q.RootDir())
			_ = os.Remove(s.q.RootDir())
		}
	}
}

func testRoute(t *testing.T, s *Server, method, url string, h http.Handler, topic, id string) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	match := mux.RouteMatch{}
	ok := s.Router.Match(req, &match)
	if !ok || match.MatchErr != nil {
		t.Fatal(match.MatchErr)
	}
	if topic != "" && match.Vars["topic"] != topic {
		t.Fatal(match.Vars["topic"])
	}
	if id != "" && req.URL.Query().Get("id") != id {
		t.Fatal(req.URL.RawQuery)
	}

	if fmt.Sprint(match.Handler) != fmt.Sprint(h) {
		t.Fatal("handlers do not match", match.Handler, fmt.Sprint(h))
	}
}
*/
