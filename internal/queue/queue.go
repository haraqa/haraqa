package queue

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/haraqa/haraqa/internal/gcmap"
	"github.com/haraqa/haraqa/internal/protocol"
	"github.com/haraqa/haraqa/internal/zeroc"
	"github.com/pkg/errors"
)

//go:generate mockgen -source queue.go -destination ../mocks/queue.go -package mocks

// Queue is the interface for dealing with topics and messages in the haraqa queue
type Queue interface {
	CreateTopic(topic []byte) error
	DeleteTopic(topic []byte) error
	ListTopics(prefix, suffix, regex string) ([][]byte, error)
	Produce(tcpConn io.Reader, topic []byte, msgSizes []int64) error
	ConsumeInfo(topic []byte, offset int64, limit int64) (filename []byte, startAt int64, msgSizes []int64, err error)
	Consume(tcpConn io.Writer, topic []byte, filename []byte, startAt int64, totalSize int64) error
	Offsets(topic []byte) (int64, int64, error)
}

// NewQueue returns a queue struct which implements the Queue interface using
// zero copy on linux systems
func NewQueue(volumes []string, maxEntries int, consumePoolSize uint64) (Queue, error) {
	restorationVolume := ""
	emptyVolumes := make([]string, 0, len(volumes))
	existingTopics := make(map[string]*produceTopic)
	for i := len(volumes) - 1; i >= 0; i-- {
		err := os.MkdirAll(volumes[i], os.ModePerm)
		if err != nil {
			return nil, err
		}
		dirs, err := ioutil.ReadDir(volumes[i])
		if err != nil {
			return nil, errors.Wrapf(err, "unable to open volume %q", volumes[i])
		}
		if len(dirs) == 0 {
			emptyVolumes = append(emptyVolumes, volumes[i])
			continue
		}
		if restorationVolume == "" {
			restorationVolume = volumes[i]
			for j := range dirs {
				if dirs[j].IsDir() {
					existingTopics[dirs[j].Name()] = nil
				}
			}
		}
	}

	if len(emptyVolumes) != len(volumes) {
		topicFiles := make(map[string][]*os.File)
		for topic := range existingTopics {

			files, err := ioutil.ReadDir(filepath.Join(restorationVolume, topic))
			if err != nil {
				return nil, err
			}
			topicFiles[topic] = make([]*os.File, 0, len(files))

			for _, file := range files {
				if file.IsDir() {
					continue
				}
				src, err := os.Open(filepath.Join(restorationVolume, topic, file.Name()))
				if err != nil {
					return nil, err
				}
				topicFiles[topic] = append(topicFiles[topic], src)
			}
		}

		for topic, files := range topicFiles {
			for i := range emptyVolumes {
				// make directory if not exists
				err := os.MkdirAll(filepath.Join(emptyVolumes[i], topic), os.ModePerm)
				if err != nil {
					return nil, err
				}

				// copy each file to its destination
				for _, src := range files {
					dst, err := os.Create(filepath.Join(emptyVolumes[i], topic, filepath.Base(src.Name())))
					if err != nil {
						return nil, err
					}
					_, err = io.Copy(dst, src)
					if err != nil {
						return nil, err
					}
					dst.Close()
				}
			}

			// close files
			for _, src := range files {
				src.Close()
			}
		}
	}

	q := &queue{
		produceTopics: existingTopics,
		consumeTopics: gcmap.NewMap(consumePoolSize),
		volumes:       volumes,
		maxEntries:    int64(maxEntries),
	}
	return q, nil
}

type queue struct {
	sync.Mutex
	volumes       []string
	maxEntries    int64
	produceTopics map[string]*produceTopic
	consumeTopics *gcmap.Map
}

type produceTopic struct {
	sync.Mutex
	relOffset int64
	offset    int64
	dat       *zeroc.MultiWriter
	messages  *zeroc.MultiWriter
}

func (q *queue) CreateTopic(topic []byte) error {
	q.Lock()
	defer q.Unlock()
	_, ok := q.produceTopics[string(topic)]
	if ok {
		return protocol.ErrTopicExists
	}

	offset := findOffset(q.volumes, string(topic))
	qt, err := newProduceTopic(q.volumes, string(topic), offset, true)
	if err != nil {
		return nil
	}

	q.produceTopics[string(topic)] = qt
	if offset > 0 {
		return protocol.ErrTopicExists
	}
	return nil
}

func findOffset(volumes []string, topic string) int64 {
	// TODO: detect missing files from a volume
	offsets := make([]int64, 0, len(volumes))
	for i := range volumes {
		dir, err := os.Open(filepath.Join(volumes[i], topic))
		if err != nil {
			continue
		}
		names, err := dir.Readdirnames(-1)
		dir.Close()
		if err != nil {
			continue
		}
		if len(names) == 0 {
			continue
		}
		var max int64
		for j := range names {
			if !strings.HasSuffix(names[j], ".dat") {
				continue
			}
			n, err := strconv.ParseInt(strings.TrimSuffix(names[j], ".dat"), 10, 64)
			if err != nil {
				continue
			}
			if n > max {
				max = n
			}
		}
		offsets = append(offsets, max)
	}
	if len(offsets) == 0 {
		return 0
	}
	offset := offsets[0]
	for i := range offsets[1:] {
		if offsets[i] < offset {
			offset = offsets[i]
		}
	}

	return offset
}

func newProduceTopic(volumes []string, topic string, offset int64, open bool) (*produceTopic, error) {
	datFiles := make([]*os.File, len(volumes))
	msgFiles := make([]*os.File, len(volumes))

	var offsets []int64
	if open {
		offsets = make([]int64, 0, len(volumes))
	}
	offsetString := formatFilename(offset)
	for i := range volumes {
		path := filepath.Join(volumes[i], topic)
		err := os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return nil, err
		}

		datFiles[i], err = os.OpenFile(filepath.Join(path, offsetString+".dat"), os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, err
		}

		if open {
			info, err := datFiles[i].Stat()
			if err == nil {
				offsets = append(offsets, int64(info.Size())/24)
			}
		}

		msgFiles[i], err = os.OpenFile(filepath.Join(path, offsetString+".hrq"), os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, err
		}
	}

	if open {
		var minOffset int64
		if len(offsets) > 0 {
			minOffset = offsets[0]
		}
		for i := range offsets {
			if offsets[i] < minOffset {
				minOffset = offsets[i]
			}
		}
	}

	datMultiWriter, err := zeroc.NewMultiWriter(datFiles...)
	if err != nil {
		return nil, err
	}

	msgMultiWriter, err := zeroc.NewMultiWriter(msgFiles...)
	if err != nil {
		return nil, err
	}

	// TODO: get relative offset of file

	return &produceTopic{
		dat:      datMultiWriter,
		messages: msgMultiWriter,
		offset:   offset,
	}, nil
}

func (q *queue) DeleteTopic(topic []byte) error {
	q.Lock()
	defer q.Unlock()
	mw, ok := q.produceTopics[string(topic)]

	if mw != nil {
		mw.dat.Close()
		mw.messages.Close()
	}
	if ok {
		delete(q.produceTopics, string(topic))
	}

	for i := range q.volumes {
		err := os.RemoveAll(filepath.Join(q.volumes[i], string(topic)))
		if err != nil {
			return err
		}
	}

	return nil
}

func (q *queue) ListTopics(prefix, suffix, regex string) ([][]byte, error) {
	var r *regexp.Regexp
	if regex != "" {
		var err error
		r, err = regexp.Compile(regex)
		if err != nil {
			return nil, err
		}
	}

	q.Lock()
	defer q.Unlock()
	output := make([][]byte, 0, len(q.produceTopics))
	for topic := range q.produceTopics {
		if prefix != "" && !strings.HasPrefix(topic, prefix) {
			continue
		}
		if suffix != "" && !strings.HasSuffix(topic, suffix) {
			continue
		}
		if r != nil && !r.MatchString(topic) {
			continue
		}
		output = append(output, []byte(topic))
	}
	return output, nil
}

func (q *queue) Produce(tcpConn io.Reader, topic []byte, msgSizes []int64) error {
	q.Lock()
	mw, ok := q.produceTopics[string(topic)]
	if !ok {
		q.Unlock()
		return protocol.ErrTopicDoesNotExist
	}
	if mw == nil {
		// open files
		offset := findOffset(q.volumes, string(topic))
		var err error
		mw, err = newProduceTopic(q.volumes, string(topic), offset, true)
		if err != nil {
			q.Unlock()
			return err
		}
		q.produceTopics[string(topic)] = mw
	}
	q.Unlock()

	mw.Lock()
	defer mw.Unlock()
	if mw.relOffset+int64(len(msgSizes)) > q.maxEntries {
		// handle creating new files for overflow
		newMW, err := newProduceTopic(q.volumes, string(topic), mw.offset, false)
		if err != nil {
			return err
		}
		mw.dat.Close()
		mw.messages.Close()
		mw.dat = newMW.dat
		mw.messages = newMW.messages
		mw.relOffset = 0
	}

	// write messages to file
	n, err := mw.messages.SetLimit(sum(msgSizes)).ReadFrom(tcpConn)
	if err != nil {
		return errors.Wrap(err, "issue writing to file")
	}

	// write message sizes to dat
	offset := mw.offset
	startAt := mw.messages.Offset()
	buf := make([]byte, 24*len(msgSizes))
	var dat [24]byte
	for i := range msgSizes {
		binary.BigEndian.PutUint64(dat[0:8], uint64(offset))
		binary.BigEndian.PutUint64(dat[8:16], uint64(startAt))
		binary.BigEndian.PutUint64(dat[16:24], uint64(msgSizes[i]))
		copy(buf[24*i:24*(i+1)], dat[:])
		startAt += msgSizes[i]
		offset++
	}

	_, err = mw.dat.Write(buf[:])
	if err != nil {
		return err
	}
	mw.dat.IncreaseOffset(int64(len(buf)))
	mw.messages.IncreaseOffset(n)
	mw.relOffset = offset - mw.offset
	mw.offset = offset
	return nil
}

func sum(s []int64) int64 {
	var out int64
	for _, v := range s {
		out += v
	}
	return out
}

// ConsumeInfo returns the info required to consume from a specific file. it
// returns the filepath of the file to be read from, the starting point to begin
// reading from, and the number and lengths of the messages to be read
func (q *queue) ConsumeInfo(topic []byte, offset int64, limit int64) ([]byte, int64, []int64, error) {
	dir := q.consumeTopics.Get(topic)
	if dir == nil {
		var err error
		dir, err = os.Open(filepath.Join(q.volumes[len(q.volumes)-1], string(topic)))
		if err != nil {
			if os.IsNotExist(err) {
				err = protocol.ErrTopicDoesNotExist
			}
			return nil, 0, nil, err
		}
	}

	// get a list of all the files
	names, err := dir.(*os.File).Readdirnames(-1)
	if err != nil {
		return nil, 0, nil, err
	}
	q.consumeTopics.Put(topic, dir)

	// find the file which matches the offset
	var baseOffset int64
	for i := range names {
		if !strings.HasSuffix(names[i], ".dat") {
			continue
		}
		o, err := strconv.ParseInt(strings.TrimSuffix(names[i], ".dat"), 10, 64)
		if err == nil {
			if o > baseOffset && (offset < 0 || o <= offset) {
				baseOffset = o
			}
		}
	}
	filename := filepath.Join(q.volumes[len(q.volumes)-1], string(topic), formatFilename(baseOffset))

	// read from .dat file
	buf, err := q.readDat(filename+".dat", offset, limit)
	if err != nil {
		return nil, 0, nil, err
	}
	if len(buf) < 24 {
		return nil, 0, nil, nil
	}

	// convert the dat file contents into a list of message sizes
	startAt := int64(binary.BigEndian.Uint64(buf[8:16]))
	msgSizes := make([]int64, 0, len(buf)/24)
	for i := 0; i < len(buf); i += 24 {
		msgSizes = append(msgSizes, int64(binary.BigEndian.Uint64(buf[i+16:i+24])))
	}

	return []byte(filename + ".hrq"), startAt, msgSizes, nil
}

func (q *queue) readDat(filename string, offset, limit int64) ([]byte, error) {
	// open the dat file
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var datSize int64
	if offset < 0 || limit < 0 {
		info, err := f.Stat()
		if err != nil {
			return nil, err
		}
		datSize = info.Size()
		if datSize < 24 {
			return nil, nil
		}
	}

	// if the offset is negative (get the latest message), find the latest message offset
	if offset < 0 {
		offset = datSize/24 - 1
	}

	// if limit is less than 1 set to dat size
	if limit < 1 {
		limit = datSize/24 + 1
	}

	// read the dat file
	buf := make([]byte, limit*24)
	n, err := f.ReadAt(buf[:], offset*24)
	if err != nil && err != io.EOF {
		return nil, err
	}
	buf = buf[:n-n%24]
	return buf, nil
}

func (q *queue) Consume(tcpConn io.Writer, topic []byte, filename []byte, startAt int64, totalSize int64) error {
	if totalSize == 0 {
		return nil
	}
	f, err := os.Open(string(filename))
	if err != nil {
		return errors.Wrapf(err, "unable to open file %q", string(filename))
	}
	defer f.Close()

	_, err = zeroc.NewReader(f, startAt, int(totalSize)).WriteTo(tcpConn)
	if err != nil {
		return err
	}
	return nil
}

func (q *queue) Offsets(topic []byte) (int64, int64, error) {
	path := filepath.Join(q.volumes[len(q.volumes)-1], string(topic))

	dir, err := os.Open(path)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			err = os.ErrNotExist
		}
		return 0, 0, err
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if len(names) == 0 {
		if err == nil || errors.Cause(err) == os.ErrNotExist {
			err = os.ErrNotExist
		}
		return 0, 0, err
	}

	var min, max int64
	min = math.MaxInt64
	var maxName string
	for j := range names {
		if !strings.HasSuffix(names[j], ".dat") {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSuffix(names[j], ".dat"), 10, 64)
		if err != nil {
			continue
		}
		if n >= max {
			max = n
			maxName = names[j]
		}
		if n < min {
			min = n
		}
	}

	if maxName != "" {
		datFile, err := os.OpenFile(filepath.Join(path, maxName), os.O_RDONLY, 0666)
		if err != nil {
			return 0, 0, err
		}

		info, err := datFile.Stat()
		if err == nil {
			max += (info.Size() / 24)
		}
		datFile.Close()
	}

	if min >= max {
		return 0, 0, os.ErrNotExist
	}

	return min, max, nil
}

func formatFilename(n int64) string {
	return fmt.Sprintf("%016d", n)
}
