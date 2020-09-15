package filequeue

import (
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
