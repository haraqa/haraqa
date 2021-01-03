package queue

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
)

const (
	infoSize = 40
	metaSize = 24
)

type File struct {
	mux *sync.Mutex
	*os.File
	extraFiles   []*os.File
	info         *[infoSize]byte
	baseID       int64
	maxEntries   int64
	numEntries   int64
	writerOffset int64
	createdAt    time.Time
	metaCache    map[int64][metaSize / 8]int64
	isClosed     bool
	used         time.Time
}

func CreateFile(dirs []string, topic string, baseID int64, maxEntries int64) (*File, error) {
	createdAt := time.Now().UTC()
	writerOffset := infoSize + maxEntries*metaSize

	// write info at start of file (number of entries 16:24)
	var info [infoSize]byte
	binary.LittleEndian.PutUint64(info[:8], uint64(baseID))
	binary.LittleEndian.PutUint64(info[8:16], uint64(maxEntries))
	binary.LittleEndian.PutUint64(info[24:32], uint64(writerOffset))
	binary.LittleEndian.PutUint64(info[32:40], uint64(createdAt.Unix()))

	initFile := func(f *os.File) error {
		if _, err := f.Write(info[:]); err != nil {
			return err
		}
		// fill with zeros
		seek := infoSize + metaSize*maxEntries - 1
		if _, err := f.Seek(seek, io.SeekStart); err != nil {
			return err
		}
		if _, err := f.Write([]byte{0}); err != nil {
			return err
		}
		return nil
	}
	closeFiles := func(files []*os.File) {
		for i := range files {
			if files[i] != nil {
				_ = files[i].Close()
			}
		}
	}

	var err error
	files := make([]*os.File, len(dirs))
	for i, dir := range dirs {
		files[i], err = os.Create(filepath.Join(dir, topic, formatName(baseID)))
		if err != nil {
			closeFiles(files)
			return nil, err
		}
		err = initFile(files[i])
		if err != nil {
			closeFiles(files)
			return nil, err
		}
	}

	return &File{
		mux:          &sync.Mutex{},
		File:         files[len(files)-1],
		extraFiles:   files[:len(files)-1],
		info:         &info,
		baseID:       baseID,
		maxEntries:   maxEntries,
		numEntries:   0,
		writerOffset: writerOffset,
		createdAt:    createdAt,
		used:         createdAt,
		metaCache:    make(map[int64][metaSize / 8]int64, maxEntries),
	}, nil
}

func OpenFile(dirs []string, topic string, baseID int64) (*File, error) {
	var err error
	f := &File{
		mux:        &sync.Mutex{},
		extraFiles: make([]*os.File, len(dirs)-1),
		used:       time.Now().UTC(),
	}
	filename := formatName(baseID)
	path := filepath.Join(dirs[len(dirs)-1], topic, filename)
	f.File, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	maxEntries, err := f.MaxEntries()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	f.metaCache = make(map[int64][metaSize / 8]int64, maxEntries)
	for i, dir := range dirs[:len(dirs)-1] {
		path := filepath.Join(dir, topic, filename)
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		f.extraFiles[i] = file
	}

	return f, nil
}

func (f *File) readInfo() error {
	var info [infoSize]byte
	if f == nil || f.File == nil {
		return errors.New("file not opened")
	}
	if _, err := f.File.ReadAt(info[:], 0); err != nil {
		return err
	}

	f.info = &info
	return nil
}

func (f *File) BaseID() (int64, error) {
	if f.baseID > 0 {
		return f.baseID, nil
	}
	if f.info == nil {
		if err := f.readInfo(); err != nil {
			return 0, err
		}
	}
	f.baseID = int64(binary.LittleEndian.Uint64((*f.info)[0:8]))
	return f.baseID, nil
}

func (f *File) MaxEntries() (int64, error) {
	if f.maxEntries > 0 {
		return f.maxEntries, nil
	}
	if f.info == nil {
		if err := f.readInfo(); err != nil {
			return 0, err
		}
	}
	f.maxEntries = int64(binary.LittleEndian.Uint64((*f.info)[8:16]))
	return f.maxEntries, nil
}

func (f *File) NumEntries() (int64, error) {
	if f.numEntries > 0 || f.info != nil {
		return f.numEntries, nil
	}
	if f.info == nil {
		if err := f.readInfo(); err != nil {
			return 0, err
		}
	}
	f.numEntries = int64(binary.LittleEndian.Uint64((*f.info)[16:24]))
	return f.numEntries, nil
}

func (f *File) WriterOffset() (int64, error) {
	if f.writerOffset > 0 {
		return f.writerOffset, nil
	}
	if f.info == nil {
		if err := f.readInfo(); err != nil {
			return 0, err
		}
	}
	f.writerOffset = int64(binary.LittleEndian.Uint64((*f.info)[24:32]))
	return f.writerOffset, nil
}

func (f *File) CreatedTime() (time.Time, error) {
	if !f.createdAt.IsZero() {
		return f.createdAt, nil
	}
	if f.info == nil {
		if err := f.readInfo(); err != nil {
			return time.Time{}, err
		}
	}

	f.createdAt = time.Unix(int64(binary.LittleEndian.Uint64((*f.info)[32:40])), 0)
	return f.createdAt, nil
}

func (f *File) Close() error {
	f.mux.Lock()
	defer f.mux.Unlock()
	f.isClosed = true
	var errs []error
	if f.File != nil {
		if err := f.File.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, f := range f.extraFiles {
		if f == nil {
			continue
		}
		if err := f.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

type Meta struct {
	sizes              []int64
	startAt, endAt     int64
	startTime, endTime time.Time
}

func (f *File) ReadMeta(id int64, limit int64) (*Meta, error) {
	if f == nil || f.File == nil {
		return nil, errors.New("file not opened")
	}
	f.used = time.Now().UTC()

	id = id - f.baseID
	if id < 0 {
		return nil, errors.New("invalid id")
	}
	numEntries, err := f.NumEntries()
	if err != nil {
		return nil, err
	}
	if id > numEntries {
		return nil, nil
	}
	if limit > numEntries-id || limit <= 0 {
		limit = numEntries - id
	}

	output := &Meta{
		sizes: make([]int64, limit),
	}

	bufOK := true
	for i := int64(0); i < limit; i++ {
		meta, ok := f.metaCache[i+id]
		if !ok {
			bufOK = false
			break
		}
		output.sizes[i] = meta[1]
		switch i {
		case 0:
			output.startAt = meta[0]
			output.startTime = time.Unix(meta[2], 0)
			if limit == 1 {
				output.endAt = meta[0] + meta[1]
				output.endTime = time.Unix(meta[2], 0)
			}
		case limit - 1:
			output.endAt = meta[0] + meta[1]
			output.endTime = time.Unix(meta[2], 0)
		}
	}
	if bufOK {
		return output, nil
	}

	buf := make([]byte, metaSize*limit)
	// TODO: check amount read
	_, err = f.File.ReadAt(buf[:], infoSize+id*metaSize)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	output.startAt = int64(binary.LittleEndian.Uint64(buf[:8]))
	output.startTime = time.Unix(int64(binary.LittleEndian.Uint64(buf[16:24])), 0)
	var off int
	for i := 0; i < len(output.sizes); i++ {
		output.sizes[i] = int64(binary.LittleEndian.Uint64(buf[off+8 : off+16]))
		off += metaSize
	}
	off -= metaSize
	output.endAt = int64(binary.LittleEndian.Uint64(buf[off:off+8])) + output.sizes[len(output.sizes)-1]
	output.endTime = time.Unix(int64(binary.LittleEndian.Uint64(buf[off+16:off+24])), 0)

	return output, nil
}

var ErrFileClosed = errors.New("file closed")

func (f *File) WriteMessages(timestamp uint64, sizes []int64, r io.Reader) (int, error) {
	f.used = time.Now().UTC()
	numEntries, err := f.NumEntries()
	if err != nil {
		return 0, err
	}
	maxEntries, err := f.MaxEntries()
	if err != nil {
		return 0, err
	}
	if numEntries == maxEntries {
		return 0, nil
	}
	writerOffset, err := f.WriterOffset()
	if err != nil {
		return 0, err
	}

	quantity := int64(len(sizes))
	if quantity > maxEntries-numEntries {
		quantity = maxEntries - numEntries
	}
	var totalSize int64
	for i := range sizes[:quantity] {
		totalSize += sizes[i]
	}
	cache := make(map[int64][metaSize / 8]int64, quantity)

	var metaOff, off int64
	var now [8]byte
	binary.LittleEndian.PutUint64(now[:], timestamp)
	buf := make([]byte, quantity*metaSize)
	for i := range sizes[:quantity] {
		cache[numEntries+int64(i)] = [metaSize / 8]int64{
			writerOffset + off,
			sizes[i],
			int64(timestamp),
		}
		binary.LittleEndian.PutUint64(buf[metaOff:metaOff+8], uint64(writerOffset+off))
		binary.LittleEndian.PutUint64(buf[metaOff+8:metaOff+16], uint64(sizes[i]))
		copy(buf[metaOff+16:metaOff+24], now[:])
		metaOff += metaSize
		off += sizes[i]
	}
	var info [16]byte
	binary.LittleEndian.PutUint64(info[:8], uint64(numEntries+quantity))
	binary.LittleEndian.PutUint64(info[8:16], uint64(writerOffset+off))

	writeFile := func(file *os.File) error {
		// write metadata
		_, err = file.WriteAt(buf, infoSize+metaSize*numEntries)
		if err != nil {
			return err
		}

		// update file info
		_, err = file.WriteAt(info[:], 16)
		if err != nil {
			return err
		}
		return nil
	}

	// get producer lock
	f.mux.Lock()
	defer f.mux.Unlock()
	if f.isClosed {
		return 0, ErrFileClosed
	}

	writers := make([]io.Writer, len(f.extraFiles)+1)
	for i := range f.extraFiles {
		_, err = f.extraFiles[i].Seek(writerOffset, io.SeekStart)
		if err != nil {
			return 0, err
		}
		writers[i] = f.extraFiles[i]
	}
	_, err = f.File.Seek(writerOffset, io.SeekStart)
	if err != nil {
		return 0, err
	}
	writers[len(writers)-1] = f.File
	w := io.MultiWriter(writers...)
	_, err = io.CopyN(w, r, totalSize)
	if err != nil {
		return 0, err
	}

	for _, file := range f.extraFiles {
		err = writeFile(file)
		if err != nil {
			return 0, err
		}
	}
	err = writeFile(f.File)
	if err != nil {
		return 0, err
	}

	for k, v := range cache {
		f.metaCache[k] = v
	}

	f.numEntries += quantity
	f.writerOffset += off

	return int(quantity), nil
}
