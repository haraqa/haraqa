package filequeue

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

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

func New(dirs ...string) (*FileQueue, error) {
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

func (q *FileQueue) ModifyTopic(topic string, request headers.ModifyRequest) (*headers.TopicInfo, error) {
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
