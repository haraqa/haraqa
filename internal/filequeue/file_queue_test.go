package filequeue

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/haraqa/haraqa/internal/headers"
	"github.com/pkg/errors"
)

func TestNewFileQueue(t *testing.T) {
	_, err := New(true, 5000)
	if err == nil {
		t.Error("expected error for missing directories")
	}

	_ = os.RemoveAll(".haraqa-newfq")
	defer os.RemoveAll(".haraqa-newfq")

	// mkdir fails
	errTest := errors.New("test error")
	osMkdir = func(name string, perm os.FileMode) error { return errTest }
	_, err = New(true, 5000, ".haraqa-newfq")
	if !errors.Is(err, errTest) {
		t.Error(err)
	}

	// mkdir succeeds but open fails
	osMkdir = func(name string, perm os.FileMode) error { return nil }
	_, err = New(true, 5000, ".haraqa-newfq")
	if !os.IsNotExist(errors.Cause(err)) {
		t.Error(err)
	}
	osMkdir = os.Mkdir

	// file is not a directory
	_, err = New(true, 5000, "file_queue.go")
	if err == nil || !strings.HasSuffix(err.Error(), "is not a directory") {
		t.Error(err)
	}

	// mkdir succeeds
	q, err := New(true, 5000, ".haraqa-newfq")
	if err != nil {
		t.Error(err)
	}
	if q.RootDir() != ".haraqa-newfq" {
		t.Error(q.RootDir())
	}
	err = q.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestFileQueue_Topics(t *testing.T) {
	dir := ".haraqa-fqtopics"
	_ = os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	q, err := New(true, 5000, dir)
	if err != nil {
		t.Error(err)
	}

	// create non-directory file
	tmp, err := os.Create(filepath.Join(dir, "thing.txt"))
	if err != nil {
		t.Error(err)
	}
	defer tmp.Close()

	// create topics
	{
		err = q.CreateTopic("newtopic")
		if err != nil {
			t.Error(err)
		}
		err = q.CreateTopic("newtopic")
		if !errors.Is(err, headers.ErrTopicAlreadyExists) {
			t.Error(err)
		}
		err = q.CreateTopic("newtopic/nested/topic")
		if err != nil {
			t.Error(err)
		}

		// mkdir error
		errTest := errors.New("mkdir error")
		osMkdir = func(name string, perm os.FileMode) error { return errTest }
		err = q.CreateTopic("newtopic")
		if !errors.Is(err, errTest) {
			t.Error(err)
		}
		osMkdir = os.Mkdir

		// mkdirall error
		osMkdirAll = func(name string, perm os.FileMode) error { return errTest }
		err = q.CreateTopic("newtopic/nested/topic")
		if !errors.Is(err, errTest) {
			t.Error(err)
		}
		osMkdirAll = os.MkdirAll

	}

	// list topics
	{
		names, err := q.ListTopics()
		if err != nil {
			t.Error(err)
		}
		if len(names) != 3 || names[0] != "newtopic" || names[1] != "newtopic/nested" || names[2] != "newtopic/nested/topic" {
			t.Error(names)
		}
	}

	// delete topics
	{
		err = q.DeleteTopic("newtopic/nested/topic")
		if err != nil {
			t.Error(err)
		}
		err = q.DeleteTopic("newtopic")
		if err != nil {
			t.Error(err)
		}
	}

	// list topics
	{
		names, err := q.ListTopics()
		if err != nil {
			t.Error(err)
		}
		if len(names) != 0 {
			t.Error(names)
		}
	}

	err = q.Close()
	if err != nil {
		t.Error(err)
	}
}

// go:generate mockgen -package queue -destination handle_consume_mocks_test.go io ReadSeeker
// go:generate goimports -w handle_consume_mocks_test.go

type readSeekCloser struct {
	io.ReadSeeker
}

func (rsc readSeekCloser) Close() error { return nil }

/*

func TestServer_HandleConsume(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	topic := "consume_topic"
	q := NewMockQueue(ctrl)
	rs := NewMockReadSeeker(ctrl)
	info := protocol.ConsumeInfo{
		Filename:  "test_file",
		File:      readSeekCloser{rs},
		Exists:    true,
		StartAt:   20,
		EndAt:     50,
		StartTime: time.Now().Add(-1 * time.Hour),
		EndTime:   time.Now(),
		Sizes:     []int64{5, 10, 15},
	}
	output := make([]byte, info.EndAt-info.StartAt)
	_, _ = rand.Read(output)
	gomock.InOrder(
		// happy path
		q.EXPECT().Consume(topic, int64(123), int64(-1)).Return(&info, nil).Times(1),
		rs.EXPECT().Seek(int64(0), io.SeekEnd).Return(int64(info.EndAt), nil).Times(1),
		rs.EXPECT().Seek(int64(0), io.SeekStart).Return(int64(0), nil).Times(1),
		rs.EXPECT().Seek(int64(info.StartAt), io.SeekStart).Return(int64(0), nil).Times(1),
		rs.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			if len(b) != int(info.EndAt-info.StartAt) {
				t.Error(len(b))
			}
			copy(b, output)
			return len(b), nil
		}).Times(1),

		// empty file
		q.EXPECT().Consume(topic, int64(123), int64(-1)).Return(&info, nil).Times(1),
		rs.EXPECT().Seek(int64(0), io.SeekEnd).Return(int64(0), nil).Times(1),
		rs.EXPECT().Seek(int64(0), io.SeekStart).Return(int64(0), nil).Times(1),

		q.EXPECT().Consume(topic, int64(123), int64(-1)).Return(&protocol.ConsumeInfo{Exists: false}, nil).Times(1),
		q.EXPECT().Consume(topic, int64(123), int64(-1)).Return(nil, protocol.ErrTopicDoesNotExist).Times(1),
		q.EXPECT().Consume(topic, int64(123), int64(-1)).Return(nil, errors.New("test consume error")).Times(1),
	)
	s := Server{q: q, metrics: noOpMetrics{}, defaultLimit: -1}

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
		err = protocol.ReadErrors(resp.Header)
		if err != protocol.ErrInvalidTopic {
			t.Fatal(err)
		}
	}

	// valid topic, invalid id
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/", bytes.NewBuffer([]byte("test body")))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic, "id": "invalid"})

		s.HandleConsume()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err != protocol.ErrInvalidMessageID {
			t.Fatal(err)
		}
	}

	// valid topic, valid id, invalid limit
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic, bytes.NewBuffer([]byte("test body")))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic, "id": "123"})
		r.Header.Set(protocol.HeaderLimit, "invalid")
		s.HandleConsume()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err != protocol.ErrInvalidHeaderLimit {
			t.Fatal(err)
		}
	}

	// valid topic, valid id, happy path
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic, bytes.NewBuffer([]byte("Hello World")))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic, "id": "123"})

		s.HandleConsume()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusPartialContent {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Header.Get(protocol.HeaderStartTime) != info.StartTime.Format(time.ANSIC) ||
			resp.Header.Get(protocol.HeaderEndTime) != info.EndTime.Format(time.ANSIC) ||
			resp.Header.Get(protocol.HeaderFileName) != info.Filename ||
			resp.Header.Get("Content-Type") != "application/octet-stream" {
			t.Fatal(resp.Header)
		}
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b, output) {
			t.Fatal("bytes not equal", b, output)
		}
	}

	// valid topic, valid id, empty file
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic, bytes.NewBuffer([]byte("Hello World")))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic, "id": "123"})

		s.HandleConsume()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
			t.Fatal(resp.Status)
		}
	}

	// valid topic, valid id, no content
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic, bytes.NewBuffer([]byte("Hello World")))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic, "id": "123"})

		s.HandleConsume()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err != protocol.ErrNoContent {
			t.Fatal(err)
		}
	}

	// valid topic, queue error: topic does not exist
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic, bytes.NewBuffer([]byte("Hello World")))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic, "id": "123"})

		s.HandleConsume()(w, r)
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
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic, bytes.NewBuffer([]byte("Hello World")))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic, "id": "123"})

		s.HandleConsume()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err.Error() != "test consume error" {
			t.Fatal(err)
		}
	}
}
*/
