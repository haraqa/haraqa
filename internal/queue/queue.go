package queue

import (
	"io"
	"os"
	"strconv"
	"sync"

	"github.com/pkg/errors"
)

type Queue struct {
	dirs        []string
	fileCache   *sync.Map
	groupCache  *sync.Map
	baseIDCache *sync.Map
	maxEntries  int64
}

func NewQueue(dirs []string, cache bool, maxEntriesPerFile int64) (*Queue, error) {
	if len(dirs) == 0 {
		return nil, errors.New("missing directories")
	}
	q := &Queue{
		dirs:       dirs,
		maxEntries: maxEntriesPerFile,
	}
	if cache {
		q.fileCache = &sync.Map{}
		q.groupCache = &sync.Map{}
		q.baseIDCache = &sync.Map{}
	}
	for _, dir := range q.dirs {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return nil, err
		}
	}
	return q, nil
}

func (q *Queue) ClearCache() error {
	if q.fileCache == nil {
		return nil
	}
	var errs []error
	q.fileCache.Range(func(key, value interface{}) bool {
		f, ok := value.(*File)
		if !ok && f == nil || f.isClosed {
			q.fileCache.Delete(key)
			if c, ok := value.(io.Closer); ok {
				errs = append(errs, c.Close())
			}
			return true
		}
		select {
		case <-f.used:
			q.fileCache.Delete(key)
			errs = append(errs, f.Close())
		default:
			f.used <- struct{}{}
		}
		return true
	})
	return firstError(errs)
}

func (q *Queue) RootDir() string {
	return q.dirs[len(q.dirs)-1]
}
func (q *Queue) Close() error {
	var errs []error
	if q.fileCache != nil {
		q.fileCache.Range(func(key, value interface{}) bool {
			q.fileCache.Delete(key)
			f, ok := value.(io.Closer)
			if ok {
				errs = append(errs, f.Close())
			}
			return true
		})
	}
	if q.baseIDCache != nil {
		q.baseIDCache.Range(func(key, _ interface{}) bool {
			q.baseIDCache.Delete(key)
			return true
		})
	}
	if q.groupCache != nil {
		q.groupCache.Range(func(key, _ interface{}) bool {
			q.groupCache.Delete(key)
			return true
		})
	}
	return firstError(errs)
}

func formatName(baseID int64) string {
	const defaultName = "0000000000000000"

	if baseID <= 0 {
		return defaultName
	}
	v := strconv.FormatInt(baseID, 10)
	if len(v) < 16 {
		v = defaultName[len(v):] + v
	}
	return v
}

func firstError(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	for i := range errs[:len(errs)-1] {
		if errs[i] != nil {
			return errs[i]
		}
	}
	return errs[len(errs)-1]
}
