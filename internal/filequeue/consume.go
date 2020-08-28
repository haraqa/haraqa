package filequeue

import (
	"encoding/binary"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/haraqa/haraqa/internal/headers"
)

func (q *FileQueue) Consume(topic string, id int64, N int64, w http.ResponseWriter) (int, error) {
	datName, err := getConsumeDat(q.consumeCache, filepath.Join(q.rootDirNames[len(q.rootDirNames)-1], topic), id)
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
			return 0, headers.ErrTopicDoesNotExist
		}
		return 0, err
	}
	defer dat.Close()

	stat, err := dat.Stat()
	if err != nil {
		return 0, err
	}
	var info consumeInfo
	if stat.Size() < id*32 {
		return 0, nil
	}

	if N < 0 {
		N = (stat.Size() - id*32) / 32
	}

	data := make([]byte, N*32)
	length, err := dat.ReadAt(data, id*32)
	if err != nil && length == 0 {
		return 0, err
	}
	N = int64(length) / 32

	info.Sizes = make([]int64, N)
	info.StartTime = time.Unix(0, int64(binary.LittleEndian.Uint64(data[8:])))
	info.StartAt = binary.LittleEndian.Uint64(data[16:])
	info.EndAt = info.StartAt
	for i := range info.Sizes {
		size := binary.LittleEndian.Uint64(data[i*32+24:])
		info.Sizes[i] = int64(size)
		info.EndAt += size
		if i == len(info.Sizes)-1 {
			info.EndTime = time.Unix(0, int64(binary.LittleEndian.Uint64(data[i*32+8:])))
		}
	}
	info.EndAt--
	info.Exists = true
	info.Filename = path + ".log"
	f, err := os.Open(info.Filename)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, headers.ErrTopicDoesNotExist
		}
		return 0, err
	}
	defer f.Close()
	info.File = f
	q.consumeResponse(w, &info)

	return len(info.Sizes), nil
}

type consumeInfo struct {
	Filename  string
	File      io.ReadSeeker
	Exists    bool
	StartAt   uint64
	EndAt     uint64
	StartTime time.Time
	EndTime   time.Time
	Sizes     []int64
}

var reqPool = sync.Pool{
	New: func() interface{} {
		return &http.Request{}
	},
}

func (q *FileQueue) consumeResponse(w http.ResponseWriter, info *consumeInfo) {

	wHeader := w.Header()
	wHeader[headers.HeaderStartTime] = []string{info.StartTime.Format(time.ANSIC)}
	wHeader[headers.HeaderEndTime] = []string{info.EndTime.Format(time.ANSIC)}
	wHeader[headers.HeaderFileName] = []string{info.Filename}
	wHeader["Content-Type"] = []string{"application/octet-stream"}
	headers.SetSizes(info.Sizes, wHeader)
	rangeHeader := "bytes=" + strconv.FormatUint(info.StartAt, 10) + "-" + strconv.FormatUint(info.EndAt, 10)
	wHeader["Range"] = []string{rangeHeader}

	req := reqPool.Get().(*http.Request)
	req.Header = wHeader
	http.ServeContent(w, req, info.Filename, info.EndTime, info.File)
	reqPool.Put(req)
}
