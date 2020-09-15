package filequeue

import (
	"encoding/binary"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/haraqa/haraqa/internal/headers"
)

// Consume copies messages from a log to the writer
func (q *FileQueue) Consume(topic string, id int64, limit int64, w http.ResponseWriter) (int, error) {
	datName, err := getConsumeDat(q.consumeCache, filepath.Join(q.rootDirNames[len(q.rootDirNames)-1], topic), topic, id)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, headers.ErrTopicDoesNotExist
		}
		return 0, err
	}
	path := filepath.Join(q.rootDirNames[len(q.rootDirNames)-1], topic, datName)
	dat, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer dat.Close()

	stat, err := dat.Stat()
	if err != nil {
		return 0, err
	}

	// check if id was less than 0
	if id < 0 {
		id = stat.Size()/datEntryLength - 1
		if id < 0 {
			return 0, nil
		}
	}

	if id > stat.Size()/datEntryLength-1 {
		base, err := strconv.ParseInt(stat.Name(), 10, 64)
		if err != nil {
			return 0, err
		}
		id = id - base
		if id > stat.Size()/datEntryLength-1 {
			return 0, nil
		}
	}

	if limit < 0 {
		limit = (stat.Size() - id*datEntryLength) / datEntryLength
	}

	data := make([]byte, limit*datEntryLength)
	length, err := dat.ReadAt(data, id*datEntryLength)
	if err != nil && length == 0 {
		return 0, err
	}
	limit = int64(length) / datEntryLength

	return q.consumeResponse(w, data, limit, path+".log")
}

func getConsumeDat(consumeCache *sync.Map, path string, topic string, id int64) (string, error) {
	exact := formatName(id)
	if consumeCache != nil {
		value, ok := consumeCache.Load(topic)
		if ok {
			names := value.([]string)
			for i := range names {
				if len(names[i]) == len(exact) && names[i] <= exact {
					return names[i], nil
				}
			}
		}
	}

	dir, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return "", err
	}
	sort.Sort(sortableDirNames(names))
	if consumeCache != nil {
		consumeCache.Store(topic, names)
	}
	if id < 0 && len(names) > 0 {
		return names[0], nil
	}

	for i := range names {
		if len(names[i]) == len(exact) && names[i] <= exact {
			return names[i], nil
		}
	}
	return formatName(0), nil
}

var reqPool = sync.Pool{
	New: func() interface{} {
		return &http.Request{}
	},
}

func (q *FileQueue) consumeResponse(w http.ResponseWriter, data []byte, limit int64, filename string) (int, error) {
	sizes := make([]int64, limit)
	startTime := time.Unix(int64(binary.LittleEndian.Uint64(data[8:])), 0)
	endTime := startTime
	startAt := binary.LittleEndian.Uint64(data[16:])
	endAt := startAt
	for i := range sizes {
		size := binary.LittleEndian.Uint64(data[i*datEntryLength+24:])
		sizes[i] = int64(size)
		endAt += size
		if i == len(sizes)-1 {
			endTime = time.Unix(int64(binary.LittleEndian.Uint64(data[i*datEntryLength+8:])), 0)
		}
	}
	endAt--
	f, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	wHeader := w.Header()
	wHeader[headers.HeaderStartTime] = []string{startTime.Format(time.ANSIC)}
	wHeader[headers.HeaderEndTime] = []string{endTime.Format(time.ANSIC)}
	wHeader[headers.HeaderFileName] = []string{filename}
	wHeader[headers.ContentType] = []string{"application/octet-stream"}
	headers.SetSizes(sizes, wHeader)
	rangeHeader := "bytes=" + strconv.FormatUint(startAt, 10) + "-" + strconv.FormatUint(endAt, 10)
	wHeader["Range"] = []string{rangeHeader}

	req := reqPool.Get().(*http.Request)
	req.Header = wHeader
	http.ServeContent(w, req, filename, endTime, f)
	reqPool.Put(req)
	return len(sizes), nil
}
