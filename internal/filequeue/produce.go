package filequeue

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/haraqa/haraqa/internal/headers"
	"github.com/pkg/errors"
)

const datEntryLength = 32

// Produce copies messages from the reader into the queue log
func (q *FileQueue) Produce(topic string, msgSizes []int64, timestamp uint64, r io.Reader) error {
	if len(msgSizes) == 0 {
		return nil
	}

	if r == nil {
		return headers.ErrInvalidBodyMissing
	}

	// lock actions on the topic
	mux, ok := q.produceLocks.Load(topic)
	if !ok {
		mux, _ = q.produceLocks.LoadOrStore(topic, &sync.Mutex{})
	}
	mux.(*sync.Mutex).Lock()
	defer mux.(*sync.Mutex).Unlock()

	// Open files
	pf, err := q.openProduceFile(topic)
	if err != nil {
		if os.IsNotExist(errors.Cause(err)) {
			err = headers.ErrTopicDoesNotExist
		}
		return errors.Wrap(err, "open producer file error")
	}
	isNewFile := pf.CurrentDatOffset == 0

	// Write logs & dats
	err = pf.Write(msgSizes, timestamp, r)
	if err != nil {
		return errors.Wrap(err, "write producer file error")
	}

	// Add back to pool
	if q.produceCache != nil {
		q.produceCache.Store(topic, pf)
	}
	if q.consumeNameCache != nil && isNewFile {
		q.consumeNameCache.Delete(topic)
	}
	return nil
}

type cacheableProduceFile struct {
	Dats, Logs       MultiWriteAtCloser
	NextID           int64
	CurrentDatOffset int64
	CurrentLogOffset int64
}

func (q *FileQueue) openProduceFile(topic string) (*cacheableProduceFile, error) {
	var pf *cacheableProduceFile
	var datName string
	var loaded bool

	// helper to close out files on error
	closeFiles := func() {
		if pf == nil {
			return
		}
		if len(pf.Dats) > 0 {
			_ = pf.Dats.Close()
			pf.Dats = nil
		}
		if len(pf.Logs) > 0 {
			_ = pf.Logs.Close()
			pf.Logs = nil
		}
		pf.CurrentDatOffset = 0
		pf.CurrentLogOffset = 0
	}

	// attempt to load from cache
	if q.produceCache != nil {
		if tmp, ok := q.produceCache.Load(topic); ok {
			if pf, ok = tmp.(*cacheableProduceFile); ok {
				// if we haven't reached the max cap, return
				if pf.CurrentDatOffset/datEntryLength < q.max {
					return pf, nil
				}

				// best effort close, prep to open a new set of files
				closeFiles()
				datName = formatName(pf.NextID)
				loaded = true
			}
		}
	}

	// find nextID based on filesystem
	if !loaded {
		pf = &cacheableProduceFile{}
		var err error
		datName, err = getLatestDat(filepath.Join(q.rootDirNames[len(q.rootDirNames)-1], topic))
		if err != nil {
			return nil, errors.Wrapf(err, "unable to open latest dat file for %q", topic)
		}
	}

	// open file set
OpenFileSet:
	for _, dir := range q.rootDirNames {
		datPath := filepath.Join(dir, topic, datName)
		dat, err := osOpenFile(datPath, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			closeFiles()
			return nil, errors.Wrapf(err, "unable to open/create file %q", datPath)
		}
		logPath := filepath.Join(dir, topic, datName+".log")
		log, err := osOpenFile(logPath, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			closeFiles()
			return nil, errors.Wrapf(err, "unable to open/create file %q", logPath)
		}
		pf.Dats = append(pf.Dats, dat)
		pf.Logs = append(pf.Logs, log)
	}

	// if we didn't load from cache, we need to stat the last file
	if !loaded {
		// stat file
		dat := pf.Dats[len(pf.Dats)-1].(*os.File)
		stat, err := dat.Stat()
		if err != nil {
			closeFiles()
			return nil, errors.Wrapf(err, "unable to stat dat file %q", dat.Name())
		}

		// read last data entry
		size := stat.Size()
		if size >= datEntryLength {
			var data [datEntryLength]byte
			_, err = dat.ReadAt(data[:], size-datEntryLength-(size%datEntryLength))
			if err != nil {
				closeFiles()
				return nil, errors.Wrap(err, "unable to write dat")
			}
			pf.NextID = int64(binary.LittleEndian.Uint64(data[0:8])) + 1
			pf.CurrentDatOffset = datEntryLength * (size / datEntryLength)
			pf.CurrentLogOffset = int64(binary.LittleEndian.Uint64(data[16:24]) + binary.LittleEndian.Uint64(data[24:32]))

			// check if this file has been filled
			if size/datEntryLength >= q.max {
				closeFiles()
				datName = formatName(pf.NextID)
				goto OpenFileSet
			}
		}
	}

	return pf, nil
}

var bufPool = sync.Pool{New: func() interface{} {
	return make([]byte, 32*1024)
}}

func (pf *cacheableProduceFile) Write(msgSizes []int64, timestamp uint64, r io.Reader) error {
	var n int
	offset := pf.CurrentLogOffset
	nextID := pf.NextID

	// get data buffer
	data := bufPool.Get().([]byte)
	if datEntryLength*len(msgSizes) > cap(data) {
		data = make([]byte, datEntryLength*len(msgSizes))
	} else {
		data = data[:datEntryLength*len(msgSizes)]
	}
	defer bufPool.Put(data)

	// create data entries
	for _, size := range msgSizes {
		binary.LittleEndian.PutUint64(data[n:], uint64(nextID))
		n += 8
		binary.LittleEndian.PutUint64(data[n:], timestamp)
		n += 8
		binary.LittleEndian.PutUint64(data[n:], uint64(offset))
		n += 8
		binary.LittleEndian.PutUint64(data[n:], uint64(size))
		n += 8
		offset += size
		nextID++
	}

	// write logs
	err := pf.Logs.CopyNAt(r, offset-pf.CurrentLogOffset, pf.CurrentLogOffset)
	if err != nil {
		return errors.Wrap(err, "unable to copy to log file")
	}

	// write dat
	err = pf.Dats.WriteAt(data, pf.CurrentDatOffset)
	if err != nil {
		return errors.Wrap(err, "unable to write to dat file")
	}

	pf.NextID = nextID
	pf.CurrentDatOffset += int64(len(data))
	pf.CurrentLogOffset = offset
	return nil
}

func getLatestDat(path string) (string, error) {
	dir, err := osOpen(path)
	if err != nil {
		return "", err
	}
	defer dir.Close()
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

// sortableDirNames attaches the methods of sort.Interface to []string, sorting in decreasing order.
type sortableDirNames []string

func (p sortableDirNames) Len() int           { return len(p) }
func (p sortableDirNames) Less(i, j int) bool { return len(p[i]) < len(p[j]) || p[i] > p[j] }
func (p sortableDirNames) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
