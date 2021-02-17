package queue

import (
	"os"
	"sync"
	"testing"

	"github.com/pkg/errors"
)

func TestNewQueue(t *testing.T) {
	name, err := os.MkdirTemp("", ".haraqa*")
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		if err := os.Remove(name); err != nil {
			t.Log(err)
		}
	}()
	t.Log("using", name)

	q, err := NewQueue([]string{name}, true, 10000)
	if err != nil {
		t.Error(err)
		return
	}
	if q.baseIDCache == nil || q.fileCache == nil || q.groupCache == nil {
		t.Error("expected cache maps to be created")
		return
	}
	if q.maxEntries != 10000 {
		t.Error(q.maxEntries)
		return
	}

	_, err = NewQueue([]string{}, true, 10000)
	if err == nil || err.Error() != "missing directories" {
		t.Error(err)
		return
	}

	_, err = NewQueue([]string{""}, true, 10000)
	if !os.IsNotExist(err) {
		t.Error(err)
		return
	}
}

func TestFormatFilename(t *testing.T) {
	name := formatName(0)
	if name != "0000000000000000" {
		t.Error(name)
	}
	name = formatName(-1)
	if name != "0000000000000000" {
		t.Error(name)
	}
	name = formatName(1234)
	if name != "0000000000001234" {
		t.Error(name)
	}
}

func TestQueue_RootDir(t *testing.T) {
	q := &Queue{
		dirs: []string{"a", "b", "c"},
	}
	if q.RootDir() != "c" {
		t.Error(q.RootDir())
	}
}

type ECloser struct {
	err error
}

func (c *ECloser) Close() error {
	return c.err
}

func TestQueue_Close(t *testing.T) {
	if err := (&Queue{}).Close(); err != nil {
		t.Error(err)
	}

	q := &Queue{
		fileCache:   &sync.Map{},
		groupCache:  &sync.Map{},
		baseIDCache: &sync.Map{},
	}
	q.fileCache.Store("key", "value")
	q.fileCache.Store("key2", &ECloser{err: os.ErrPermission})
	q.fileCache.Store("key3", &ECloser{err: os.ErrPermission})
	q.groupCache.Store("key", "group")
	q.baseIDCache.Store("key", 1234)

	err := q.Close()
	if !errors.Is(err, os.ErrPermission) {
		t.Error(err)
	}
}

func TestQueue_ClearCache(t *testing.T) {
	q := &Queue{}

	if err := q.ClearCache(); err != nil {
		t.Error(err)
	}

	q.fileCache = &sync.Map{}
	q.fileCache.Store("key", "value")
	q.fileCache.Store("key2", &File{
		used: make(chan struct{}, 1),
	})
	q.fileCache.Store("key3", &File{
		isClosed: true,
	})

	if err := q.ClearCache(); err != nil {
		t.Error(err)
		return
	}
	var count int
	q.fileCache.Range(func(key, value interface{}) bool {
		count++
		if key != "key2" {
			t.Error("unexpected key", key)
		}
		return true
	})
	if count != 1 {
		t.Error("invalid count", count)
		return
	}
	count = 0
	if err := q.ClearCache(); err != nil {
		t.Error(err)
		return
	}
	q.fileCache.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	if count != 0 {
		t.Error("invalid count", count)
	}

	q.fileCache.Store("key4", &ECloser{err: os.ErrPermission})
	err := q.ClearCache()
	if !errors.Is(err, os.ErrPermission) {
		t.Error(err)
	}
	q.fileCache.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	if count != 0 {
		t.Error("invalid count", count)
	}
}
