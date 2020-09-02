package filequeue

import (
	"encoding/binary"
	"io"
	"os"
	"sync"

	"github.com/haraqa/haraqa/internal/headers"
)

func (q *FileQueue) Produce(topic string, msgSizes []int64, timestamp uint64, r io.Reader) error {
	if len(msgSizes) == 0 {
		return nil
	}
	mux, _ := q.produceLocks.LoadOrStore(topic, &sync.Mutex{})
	mux.(*sync.Mutex).Lock()
	defer mux.(*sync.Mutex).Unlock()

	fs, err := q.loadLatest(topic)
	if err != nil {
		if os.IsNotExist(err) {
			return headers.ErrTopicDoesNotExist
		}
		return err
	}

	data := make([]byte, 32*len(msgSizes))
	var n int
	var id uint64
	var totalSize int64
	for _, size := range msgSizes {
		binary.LittleEndian.PutUint64(data[n:], id)
		n += 8
		binary.LittleEndian.PutUint64(data[n:], timestamp)
		n += 8
		binary.LittleEndian.PutUint64(data[n:], uint64(fs.FileOffset+totalSize))
		n += 8
		binary.LittleEndian.PutUint64(data[n:], uint64(size))
		n += 8
		id++
		totalSize += size
	}

	if err := fs.WriteLogs(r, totalSize); err != nil {
		return err
	}

	return fs.WriteDats(data)
}

func (q *FileQueue) loadLatest(topic string) (*LogFiles, error) {
	var fs *LogFiles
	if q.produceCache == nil {
		fs = &LogFiles{}
		return fs, fs.Open(q.rootDirNames, topic, q.max)
	}

	value, ok := q.produceCache.Load(topic)
	if ok {
		if value.(*LogFiles).Entries < q.max {
			return value.(*LogFiles), nil
		}
		value.(*LogFiles).Close()
	}

	fs = &LogFiles{}
	err := fs.Open(q.rootDirNames, topic, q.max)
	if err != nil {
		return nil, err
	}
	q.produceCache.Store(topic, fs)

	return fs, nil
}
