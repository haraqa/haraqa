package queue

import (
	"io"
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
	if len(volumes) == 0 {
		return nil, errors.New("missing volumes from NewQueue call")
	}

	err := checkForDuplicates(volumes)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create new queue")
	}

	restorationVolume := ""
	emptyVolumes := make([]string, 0, len(volumes))
	existingTopics := make(map[string]*produceTopic)
	for i := len(volumes) - 1; i >= 0; i-- {
		err := os.MkdirAll(volumes[i], os.ModePerm)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to instantiate volume %s", volumes[i])
		}

		topics := getVolumeTopics(volumes[i])
		if len(topics) == 0 {
			emptyVolumes = append(emptyVolumes, volumes[i])
			continue
		}

		restorationVolume = volumes[i]
		existingTopics = topics
		emptyVolumes = append(emptyVolumes, volumes[:i]...)
		break
	}

	if len(emptyVolumes) != len(volumes) {
		err = restore(emptyVolumes, restorationVolume)
		if err != nil {
			return nil, errors.Wrap(err, "restore data error")
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
			if !strings.HasSuffix(names[j], datFileExt) {
				continue
			}
			n, err := strconv.ParseInt(strings.TrimSuffix(names[j], datFileExt), 10, 64)
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

		datFiles[i], err = os.OpenFile(filepath.Join(path, offsetString+datFileExt), os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, err
		}

		if open {
			info, err := datFiles[i].Stat()
			if err == nil {
				offsets = append(offsets, int64(info.Size())/24)
			}
		}

		msgFiles[i], err = os.OpenFile(filepath.Join(path, offsetString+hrqFileExt), os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, err
		}
	}

	if open && len(offsets) > 0 {
		minOffset := offsets[0]
		for i := range offsets[1:] {
			if offsets[i] < minOffset {
				minOffset = offsets[i]
			}
		}
		offset = minOffset
	}

	datMultiWriter, err := zeroc.NewMultiWriter(datFiles...)
	if err != nil {
		return nil, err
	}

	msgMultiWriter, err := zeroc.NewMultiWriter(msgFiles...)
	if err != nil {
		return nil, err
	}

	p := &produceTopic{
		dat:      datMultiWriter,
		messages: msgMultiWriter,
		offset:   offset,
	}

	if offset > 0 {
		// get relative offset of file
		var entry datum
		_, err = datFiles[len(datFiles)-1].Seek((offset-1)*24, io.SeekStart)
		if err != nil {
			return nil, err
		}
		_, err = io.ReadFull(datFiles[len(datFiles)-1], entry[:])
		if err != nil {
			return nil, err
		}
		p.relOffset = entry.GetStartAt() + entry.GetMsgSize()
	}

	return p, nil
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
	mw.Lock()
	defer mw.Unlock()
	q.Unlock()

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

	err = writeDat(mw.dat, offset, startAt, msgSizes)
	if err != nil {
		return err
	}

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
		if !strings.HasSuffix(names[i], datFileExt) {
			continue
		}
		o, err := strconv.ParseInt(strings.TrimSuffix(names[i], datFileExt), 10, 64)
		if err == nil {
			if o > baseOffset && (offset < 0 || o <= offset) {
				baseOffset = o
			}
		}
	}
	filename := filepath.Join(q.volumes[len(q.volumes)-1], string(topic), formatFilename(baseOffset))

	// read from .dat file
	f, err := os.Open(filename + datFileExt)
	if err != nil {
		return nil, 0, nil, err
	}
	startAt, msgSizes, err := readDat(f, offset, limit)
	if err != nil {
		return nil, 0, nil, err
	}

	return []byte(filename + hrqFileExt), startAt, msgSizes, nil
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
		if !strings.HasSuffix(names[j], datFileExt) {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSuffix(names[j], datFileExt), 10, 64)
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
