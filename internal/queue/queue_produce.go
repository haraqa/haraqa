package queue

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/haraqa/haraqa/internal/headers"
)

func (q *Queue) Produce(topic string, msgSizes []int64, timestamp uint64, r io.Reader) error {
	if len(msgSizes) == 0 {
		return nil
	}
	baseID, err := q.getLatestBaseID(topic)
	if err != nil {
		return err
	}
	return q.produce(topic, msgSizes, timestamp, r, baseID)
}

func (q *Queue) getLatestBaseID(topic string) (int64, error) {
	if q.baseIDCache != nil {
		v, found := q.baseIDCache.Load(topic)
		if found {
			return v.(int64), nil
		}
	}

	dir, err := os.Open(filepath.Join(q.RootDir(), topic))
	if err != nil {
		return 0, err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return 0, err
	}
	if len(names) == 0 {
		if q.baseIDCache != nil {
			q.baseIDCache.Store(topic, int64(0))
		}
		return 0, nil
	}
	sort.Strings(names)
	baseID, err := strconv.ParseInt(names[len(names)-1], 10, 64)
	if err != nil {
		return 0, err
	}
	if q.baseIDCache != nil {
		q.baseIDCache.Store(topic, baseID)
	}
	return baseID, nil
}

func (q *Queue) produce(topic string, msgSizes []int64, timestamp uint64, r io.Reader, baseID int64) error {
	var (
		f   *File
		err error
		key string
	)
	if q.fileCache != nil {
		key = filepath.Join(q.RootDir(), topic, formatName(baseID))
		v, found := q.fileCache.Load(key)
		if found {
			f, _ = v.(*File)
		}
	}

	if f == nil {
		f, err = OpenFile(q.dirs, topic, baseID)
		if os.IsNotExist(err) {
			f, err = CreateFile(q.dirs, topic, baseID, q.maxEntries)
			if os.IsNotExist(err) {
				return headers.ErrTopicDoesNotExist
			}
		}
		if err != nil {
			return err
		}
		if q.fileCache == nil {
			defer f.Close()
		} else {
			q.fileCache.Store(key, f)
		}
	}

	n, err := f.WriteMessages(timestamp, msgSizes, r)
	if err != nil {
		return err
	}

	if n < len(msgSizes) {
		if f.numEntries == f.maxEntries {
			baseID += f.maxEntries
			if q.baseIDCache != nil {
				q.baseIDCache.Store(topic, baseID)
			}
		}
		return q.produce(topic, msgSizes[n:], timestamp, r, baseID)
	}
	return nil
}
