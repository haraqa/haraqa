package queue

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
)

var _ Queue = &FileQueue{}

type FileQueue struct {
	rootDirNames []string
	rootDirs     []*os.File
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
	err := fs.Open(q.rootDirNames, topic)
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
	name := filepath.Join(q.rootDirNames[len(q.rootDirNames)-1], topic, "tmp.dat")
	dat, err := os.Open(name)
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
	info.Filename = filepath.Join(q.rootDirNames[len(q.rootDirNames)-1], topic, "tmp.log")
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

func (fs *LogFiles) Open(dirs []string, topic string) error {
	// open last dat file
	name := filepath.Join(dirs[len(dirs)-1], topic, "tmp.dat")
	dat, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return errors.Wrapf(err, "unable to open file %q", name)
	}

	// get length
	info, err := dat.Stat()
	if err != nil {
		return errors.Wrapf(err, "unable to get file length %q", name)
	}
	if info.Size() >= 32 {
		// roll file to the last available entry
		_, err = dat.Seek(info.Size()-32-info.Size()%32, io.SeekStart)
		if err != nil {
			return errors.Wrapf(err, "unable to seek on %q", name)
		}

		// lastEntry has 32 bytes: id, timestamp, file startAt, length
		var lastEntry [32]byte
		n, err := dat.Read(lastEntry[:])
		if err != nil && n < 32 {
			err = errors.Wrapf(err, "unable to read %q %d", name, n)
			return err
		}

		fs.FileOffset = int64(binary.LittleEndian.Uint64(lastEntry[16:24]))
		fs.FileOffset += int64(binary.LittleEndian.Uint64(lastEntry[24:32]))
	}

	fs.Dats = make([]*os.File, 0, len(dirs))
	fs.Logs = make([]*os.File, 0, len(dirs))

	for i := range dirs {
		name := filepath.Join(dirs[i], topic, "tmp.log")
		log, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			return errors.Wrapf(err, "unable to open file %q", name)
		}
		_, err = log.Seek(fs.FileOffset, io.SeekStart)
		if err != nil {
			return errors.Wrapf(err, "unable to set file offset %q", name)
		}
		fs.Logs = append(fs.Logs, log)

		if i == len(dirs)-1 {
			fs.Dats = append(fs.Dats, dat)
			continue
		}

		name = filepath.Join(dirs[i], topic, "tmp.dat")
		f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			return errors.Wrapf(err, "unable to open file %q", name)
		}
		_, err = f.Seek(fs.FileOffset, io.SeekStart)
		if err != nil {
			return errors.Wrapf(err, "unable to set file offset %q", name)
		}
		fs.Dats = append(fs.Dats, f)
	}
	return nil
}
