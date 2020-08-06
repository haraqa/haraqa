package queue

import (
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/haraqa/haraqa/internal/headers"
	"github.com/pkg/errors"
)

type FileQueue struct {
	rootDirNames []string
	rootDirs     []*os.File
	max          int64
	produceLocks sync.Map
	produceCache cache
	consumeCache cache
}

type cache interface {
	Load(key interface{}) (interface{}, bool)
	Store(key, value interface{})
}

func NewFileQueue(dirs ...string) (*FileQueue, error) {
	if len(dirs) == 0 {
		return nil, errors.New("at least one directory must be given")
	}

	dirNames := make([]string, 0, len(dirs))
	dirFiles := make([]*os.File, 0, len(dirs))

	for _, dir := range dirs {
		dir = filepath.Clean(dir)

		f, err := os.Open(dir)
		if os.IsNotExist(err) {
			err = os.Mkdir(dir, os.ModePerm)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to create queue directory %q", dir)
			}
			f, err = os.Open(dir)
		}
		if err != nil {
			return nil, errors.Wrapf(err, "invalid queue directory %q", dir)
		}
		info, err := f.Stat()
		if err != nil {
			return nil, errors.Wrapf(err, "unable to read queue directory %q", dir)
		}
		if !info.IsDir() {
			return nil, errors.Errorf("path %q is not a directory", dir)
		}

		dirFiles = append(dirFiles, f)
		dirNames = append(dirNames, dir)
	}

	return &FileQueue{
		rootDirNames: dirNames,
		rootDirs:     dirFiles,
		max:          5000,
		produceCache: &sync.Map{},
		consumeCache: &sync.Map{},
	}, nil
}

func (q *FileQueue) Close() error {
	for i := range q.rootDirs {
		q.rootDirs[i].Close()
	}
	return nil
}

func (q *FileQueue) RootDir() string {
	return q.rootDirNames[len(q.rootDirNames)-1]
}

func (q *FileQueue) ListTopics() ([]string, error) {
	return q.rootDirs[len(q.rootDirs)-1].Readdirnames(-1)
}

func (q *FileQueue) CreateTopic(topic string) error {
	for _, name := range q.rootDirNames {
		err := os.Mkdir(filepath.Join(name, topic), os.ModePerm)
		if os.IsExist(err) {
			return headers.ErrTopicAlreadyExists
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *FileQueue) DeleteTopic(topic string) error {
	for _, name := range q.rootDirNames {
		os.RemoveAll(filepath.Join(name, topic))
	}
	return nil
}

func (q *FileQueue) InspectTopic(topic string) (*headers.TopicInfo, error) {
	return nil, nil
}

func (q *FileQueue) TruncateTopic(topic string, id int64) (*headers.TopicInfo, error) {
	return nil, nil
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

func (q *FileQueue) Produce(topic string, msgSizes []int64, r io.Reader) error {
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

	now := uint64(time.Now().Unix())
	data := make([]byte, 32*len(msgSizes))
	var n int
	var id uint64
	var totalSize int64
	for _, size := range msgSizes {
		binary.LittleEndian.PutUint64(data[n:], id)
		n += 8
		binary.LittleEndian.PutUint64(data[n:], now)
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

type LogFiles struct {
	Dats       []*os.File
	Logs       []*os.File
	FileOffset int64
	Entries    int64
}

func (fs *LogFiles) Close() {
	for _, f := range fs.Dats {
		f.Close()
	}
	for _, f := range fs.Logs {
		f.Close()
	}
}

func (fs *LogFiles) WriteDats(b []byte) error {
	for _, f := range fs.Dats {
		_, err := f.Write(b)
		if err != nil {
			return err
		}
	}
	return nil
}

// bufPool is used to reduce heap allocations due to io.CopyBuffer
var bufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 32*1024)
	},
}

func (fs *LogFiles) WriteLogs(r io.Reader, totalSize int64) error {
	writers := make([]io.Writer, len(fs.Logs))
	for i := range fs.Logs {
		writers[i] = fs.Logs[i]
	}
	mw := io.MultiWriter(writers...)

	buf := bufPool.Get().([]byte)
	n, err := io.CopyBuffer(mw, r, buf)
	bufPool.Put(buf)
	if err != nil {
		return err
	}
	if n < totalSize {
		return errors.Errorf("invalid size written %d/%d", n, totalSize)
	}
	return nil
}

func formatName(baseID int64) string {
	return fmt.Sprintf("%016d", baseID)
}

func getLatestDat(path string) (string, error) {
	dir, err := os.Open(path)
	if err != nil {
		return "", err
	}
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return "", err
	}
	sort.Sort(sortableDirNames(names))
	for i := range names {
		if !strings.HasSuffix(names[i], ".log") {
			return names[i], nil
		}
	}
	return formatName(0), nil
}

func getConsumeDat(consumeCache cache, path string, id int64) (string, error) {
	exact := formatName(id)
	value, ok := consumeCache.Load(path)
	if ok {
		names := value.([]string)
		for i := range names {
			if len(names[i]) == len(exact) && names[i] <= exact {
				return names[i], nil
			}
		}
	}

	dir, err := os.Open(path)
	if err != nil {
		return "", err
	}
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return "", err
	}
	sort.Sort(sortableDirNames(names))
	consumeCache.Store(path, names)
	for i := range names {
		if len(names[i]) == len(exact) && names[i] <= exact {
			return names[i], nil
		}
	}
	return formatName(0), nil
}

// sortableDirNames attaches the methods of sort.Interface to []string, sorting in decreasing order.
type sortableDirNames []string

func (p sortableDirNames) Len() int           { return len(p) }
func (p sortableDirNames) Less(i, j int) bool { return len(p[i]) < len(p[j]) || p[i] > p[j] }
func (p sortableDirNames) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (fs *LogFiles) Open(dirs []string, topic string, max int64) error {
	datName, err := getLatestDat(filepath.Join(dirs[len(dirs)-1], topic))
	if err != nil {
		return err
	}

	// open last dat file
	path := filepath.Join(dirs[len(dirs)-1], topic, datName)
	dat, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return errors.Wrapf(err, "unable to open file %q", path)
	}

	// get length
	info, err := dat.Stat()
	if err != nil {
		return errors.Wrapf(err, "unable to get file length %q", path)
	}

	// create a new dat as needed
	if info.Size()/32 >= max {
		baseID, err := strconv.ParseInt(datName, 10, 64)
		if err != nil {
			return err
		}
		baseID += info.Size() / 32

		_ = dat.Close()
		datName = formatName(baseID)
		path = filepath.Join(dirs[len(dirs)-1], topic, datName)
		dat, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			return errors.Wrapf(err, "unable to open file %q", path)
		}
		fs.FileOffset = 0

	} else if info.Size() >= 32 {
		// roll file to the last available entry
		_, err = dat.Seek(info.Size()-32-info.Size()%32, io.SeekStart)
		if err != nil {
			return errors.Wrapf(err, "unable to seek on %q", path)
		}

		// lastEntry has 32 bytes: id, timestamp, file startAt, length
		var lastEntry [32]byte
		n, err := dat.Read(lastEntry[:])
		if err != nil && n < 32 {
			err = errors.Wrapf(err, "unable to read %q %d", path, n)
			return err
		}

		fs.FileOffset = int64(binary.LittleEndian.Uint64(lastEntry[16:24]))
		fs.FileOffset += int64(binary.LittleEndian.Uint64(lastEntry[24:32]))
	}

	fs.Dats = make([]*os.File, 0, len(dirs))
	fs.Logs = make([]*os.File, 0, len(dirs))

	for i := range dirs {
		path = filepath.Join(dirs[i], topic, datName)
		log, err := os.OpenFile(path+".log", os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			return errors.Wrapf(err, "unable to open file %q", path+".log")
		}
		_, err = log.Seek(fs.FileOffset, io.SeekStart)
		if err != nil {
			return errors.Wrapf(err, "unable to set file offset %q", path+".log")
		}
		fs.Logs = append(fs.Logs, log)

		if i == len(dirs)-1 {
			fs.Dats = append(fs.Dats, dat)
			continue
		}

		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			return errors.Wrapf(err, "unable to open file %q", path)
		}
		_, err = f.Seek(fs.FileOffset, io.SeekStart)
		if err != nil {
			return errors.Wrapf(err, "unable to set file offset %q", path)
		}
		fs.Dats = append(fs.Dats, f)
	}

	fs.Entries = info.Size() / 32
	return nil
}
