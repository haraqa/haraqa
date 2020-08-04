package server

import (
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/gorilla/mux"

	"github.com/golang/mock/gomock"
	"github.com/haraqa/haraqa/server/queue"

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
		q := queue.NewMockQueue(ctrl)
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
		_, err := NewServer(WithDirs("invalid/folder/doesnt/exist/..."))
		if !strings.HasSuffix(err.Error(), "no such file or directory") {
			t.Fatal(errors.Unwrap(err))
		}
	}

	// verify routes
	{
		q := queue.NewMockQueue(ctrl)
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
		testRoute(t, s, http.MethodGet, "/topics/inspected_topic", s.HandleInspectTopic(), "inspected_topic", "")

		// produce to topic
		testRoute(t, s, http.MethodPost, "/topics/produce_topic", s.HandleProduce(), "produce_topic", "")

		// consume from topic
		testRoute(t, s, http.MethodGet, "/topics/consume_topic/1234", s.HandleConsume(), "consume_topic", "1234")
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
		if reflect.TypeOf(s.q) != reflect.TypeOf(&queue.FileQueue{}) {
			t.Error(reflect.TypeOf(s.q), reflect.TypeOf(&queue.FileQueue{}))
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
	if id != "" && match.Vars["id"] != id {
		t.Fatal(match.Vars["id"])
	}

	if fmt.Sprint(match.Handler) != fmt.Sprint(h) {
		t.Fatal("handlers do not match")
	}
}
