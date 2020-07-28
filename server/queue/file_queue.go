package queue

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

var _ Queue = &FileQueue{}

type FileQueue struct {
	rootDirNames []string
	rootDirs     []*os.File
	max          int64
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

func (q *FileQueue) InspectTopic(topic string) (*TopicInfo, error) {
	return nil, nil
}

func (q *FileQueue) TruncateTopic(topic string, id int64) (*TopicInfo, error) {
	return nil, nil
}

func (q *FileQueue) Produce(topic string, msgSizes []int64, r io.Reader) error {
	if len(msgSizes) == 0 {
		return nil
	}

	var fs LogFiles
	err := fs.Open(q.rootDirNames, topic, q.max)
	defer fs.Close()
	if err != nil {
		return err
	}

	now := uint64(time.Now().UnixNano())
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

	if err = fs.WriteLogs(r, totalSize); err != nil {
		return err
	}

	return fs.WriteDats(data)
}

func (q *FileQueue) Consume(topic string, id int64, N int64) (*ConsumeInfo, error) {
	datName, err := getConsumeDat(filepath.Join(q.rootDirNames[len(q.rootDirNames)-1], topic), id)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(q.rootDirNames[len(q.rootDirNames)-1], topic, datName)
	dat, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer dat.Close()

	stat, err := dat.Stat()
	if err != nil {
		return nil, err
	}
	var info ConsumeInfo
	if stat.Size() < id*32 {
		return &info, nil
	}

	if N < 0 {
		N = (stat.Size() - id*32) / 32
	}

	data := make([]byte, N*32)
	length, err := dat.ReadAt(data, id*32)
	if err != nil && length == 0 {
		return nil, err
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
	info.File, err = os.Open(info.Filename)
	if err != nil {
		return nil, err
	}

	return &info, nil
}

type LogFiles struct {
	Dats       []*os.File
	Logs       []*os.File
	FileOffset int64
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

func (fs *LogFiles) WriteLogs(r io.Reader, totalSize int64) error {
	writers := make([]io.Writer, len(fs.Logs))
	for i := range fs.Logs {
		writers[i] = fs.Logs[i]
	}
	mw := io.MultiWriter(writers...)

	n, err := io.Copy(mw, r)
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
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	for i := range names {
		if !strings.HasSuffix(names[i], ".log") {
			return names[i], nil
		}
	}
	return formatName(0), nil
}

func getConsumeDat(path string, id int64) (string, error) {
	dir, err := os.Open(path)
	if err != nil {
		return "", err
	}
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return "", err
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))

	exact := formatName(id)
	for i := range names {
		if !strings.HasSuffix(names[i], ".log") && names[i] <= exact {
			return names[i], nil
		}
	}
	return formatName(0), nil
}

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
	return nil
}
