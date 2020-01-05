package broker

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/haraqa/haraqa/broker/zeroc"
	"github.com/haraqa/haraqa/protocol"
	"github.com/pkg/errors"
)

//go:generate mockgen -source queue.go -destination queue_mock.go -package broker

// Queue is the interface for dealing with topics and messages in the haraqa queue
type Queue interface {
	CreateTopic(topic []byte) error
	DeleteTopic(topic []byte) error
	ListTopics() ([][]byte, error)
	Produce(tcpConn *os.File, topic []byte, msgSizes []int64) error
	ConsumeData(topic []byte, offset int64, maxBatchSize int64) (filename []byte, startAt int64, msgSizes []int64, err error)
	Consume(tcpConn *os.File, topic []byte, filename []byte, startAt int64, totalSize int64) error
	Offsets(topic []byte) (int64, int64, error)
}

// NewQueue returns a queue struct which implements the Queue interface using
// zero copy on linux systems
func NewQueue(volumes []string, maxEntries int) (Queue, error) {
	restorationVolume := ""
	emptyVolumes := make([]string, 0, len(volumes))
	existingTopics := make(map[string]*queueTopic)
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
					dst, err := os.Create(filepath.Join(emptyVolumes[i], topic, src.Name()))
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
		topics:     existingTopics,
		volumes:    volumes,
		maxEntries: uint64(maxEntries),
	}
	return q, nil
}

type queue struct {
	sync.Mutex
	topics     map[string]*queueTopic
	volumes    []string
	maxEntries uint64
}

type queueTopic struct {
	sync.Mutex
	relOffset uint64
	offset    uint64
	data      *zeroc.MultiWriter
	messages  *zeroc.MultiWriter
}

func (q *queue) CreateTopic(topic []byte) error {
	q.Lock()
	defer q.Unlock()
	_, ok := q.topics[string(topic)]
	if ok {
		return protocol.ErrTopicExists
	}

	offset := findQueueOffset(q.volumes, string(topic))
	qt, err := newQueueTopic(q.volumes, string(topic), offset, true)
	if err != nil {
		return nil
	}

	q.topics[string(topic)] = qt
	if offset > 0 {
		return protocol.ErrTopicExists
	}
	return nil
}

func findQueueOffset(volumes []string, topic string) uint64 {
	// TODO: detect missing files from a volume
	offsets := make([]uint64, 0, len(volumes))
	for i := range volumes {
		files, err := ioutil.ReadDir(filepath.Join(volumes[i], topic))
		if err != nil {
			continue
		}
		if len(files) == 0 {
			continue
		}
		var max uint64
		for j := range files {
			if !strings.HasSuffix(files[j].Name(), ".dat") {
				continue
			}
			n, err := strconv.ParseUint(strings.TrimSuffix(files[j].Name(), ".dat"), 10, 64)
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

func newQueueTopic(volumes []string, topic string, offset uint64, open bool) (*queueTopic, error) {
	dataFiles := make([]*os.File, len(volumes))
	msgFiles := make([]*os.File, len(volumes))
	offsetString := strconv.FormatUint(offset, 10)
	var offsets []uint64
	if open {
		offsets = make([]uint64, 0, len(volumes))
	}
	for i := range volumes {
		err := os.MkdirAll(filepath.Join(volumes[i], topic), os.ModePerm)
		if err != nil {
			return nil, err
		}

		dataFiles[i], err = os.OpenFile(filepath.Join(volumes[i], topic, offsetString+".dat"), os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, err
		}

		if open {
			info, err := dataFiles[i].Stat()
			if err == nil {
				offsets = append(offsets, uint64(info.Size())/24)
			}
		}

		msgFiles[i], err = os.OpenFile(filepath.Join(volumes[i], topic, offsetString+".hrq"), os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, err
		}
	}

	if open {
		var minOffset uint64
		if len(offsets) > 0 {
			minOffset = offsets[0]
		}
		for i := range offsets {
			if offsets[i] < minOffset {
				minOffset = offsets[i]
			}
		}
	}

	dataMultiWriter, err := zeroc.NewMultiWriter(dataFiles...)
	if err != nil {
		return nil, err
	}

	msgMultiWriter, err := zeroc.NewMultiWriter(msgFiles...)
	if err != nil {
		return nil, err
	}

	// TODO: get relative offset of file

	return &queueTopic{
		data:     dataMultiWriter,
		messages: msgMultiWriter,
		offset:   offset,
	}, nil
}

func (q *queue) DeleteTopic(topic []byte) error {
	q.Lock()
	defer q.Unlock()
	mw, ok := q.topics[string(topic)]

	if mw != nil {
		mw.data.Close()
		mw.messages.Close()
	}
	if ok {
		delete(q.topics, string(topic))
	}

	for i := range q.volumes {
		err := os.RemoveAll(filepath.Join(q.volumes[i], string(topic)))
		if err != nil {
			return err
		}
	}

	return nil
}

func (q *queue) ListTopics() ([][]byte, error) {
	q.Lock()
	defer q.Unlock()
	output := make([][]byte, 0, len(q.topics))
	for topic := range q.topics {
		output = append(output, []byte(topic))
	}
	return output, nil
}

func (q *queue) Produce(tcpConn *os.File, topic []byte, msgSizes []int64) error {
	q.Lock()
	mw, ok := q.topics[string(topic)]
	if !ok {
		q.Unlock()
		return protocol.ErrTopicDoesNotExist
	}
	if mw == nil {
		// open files
		offset := findQueueOffset(q.volumes, string(topic))
		var err error
		mw, err = newQueueTopic(q.volumes, string(topic), offset, true)
		if err != nil {
			q.Unlock()
			return err
		}
		q.topics[string(topic)] = mw
	}
	q.Unlock()

	mw.Lock()
	defer mw.Unlock()
	if mw.relOffset+uint64(len(msgSizes)) > q.maxEntries {
		// handle creating new files for overflow
		newMW, err := newQueueTopic(q.volumes, string(topic), mw.offset, false)
		if err != nil {
			return err
		}
		mw.data.Close()
		mw.messages.Close()
		mw.data = newMW.data
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
		binary.BigEndian.PutUint64(dat[0:8], offset)
		binary.BigEndian.PutUint64(dat[8:16], uint64(startAt))
		binary.BigEndian.PutUint64(dat[16:24], uint64(msgSizes[i]))
		copy(buf[24*i:24*(i+1)], dat[:])
		startAt += msgSizes[i]
		offset++
	}

	_, err = mw.data.Write(buf[:])
	if err != nil {
		return err
	}
	mw.data.IncreaseOffset(int64(len(buf)))
	mw.messages.IncreaseOffset(n)
	mw.relOffset = offset - mw.offset
	mw.offset = offset
	return nil
}

func (q *queue) ConsumeData(topic []byte, offset int64, maxBatchSize int64) ([]byte, int64, []int64, error) {
	// get a list of all the files
	files, err := ioutil.ReadDir(filepath.Join(q.volumes[len(q.volumes)-1], string(topic)))
	if err != nil {
		return nil, 0, nil, err
	}

	// find the file which matches the offset
	var baseOffset int64
	for i := range files {
		if !strings.HasSuffix(files[i].Name(), ".dat") {
			continue
		}
		o, err := strconv.ParseInt(strings.TrimSuffix(files[i].Name(), ".dat"), 10, 64)
		if err == nil {
			if o > baseOffset && (offset < 0 || o <= offset) {
				baseOffset = o
			}
		}
	}
	o := strconv.FormatInt(baseOffset, 10)
	filename := filepath.Join(q.volumes[len(q.volumes)-1], string(topic), o+".dat")

	// open the data file
	f, err := os.Open(filename)
	if err != nil {
		return nil, 0, nil, err
	}
	defer f.Close()

	// if the offset is negative (get the latest message), find the latest message offset
	if offset < 0 {
		info, err := f.Stat()
		if err != nil {
			return nil, 0, nil, err
		}
		if info.Size() < 24 {
			return nil, 0, nil, nil
		}

		offset = info.Size()/24 - 1
	}

	// read the data file
	buf := make([]byte, maxBatchSize*24)
	n, err := f.ReadAt(buf[:], offset*24)
	if err != nil && err != io.EOF {
		return nil, 0, nil, err
	}
	buf = buf[:n-n%24]
	if len(buf) < 24 {
		return nil, 0, nil, nil
	}

	// convert the data file contents into a list of message sizes
	startAt := int64(binary.BigEndian.Uint64(buf[8:16]))
	msgSizes := make([]int64, 0, len(buf)/24)
	for i := 0; i < len(buf); i += 24 {
		msgSizes = append(msgSizes, int64(binary.BigEndian.Uint64(buf[i+16:i+24])))
	}

	return []byte(filepath.Join(q.volumes[len(q.volumes)-1], string(topic), o+".hrq")), startAt, msgSizes, nil
}

func (q *queue) Consume(tcpConn *os.File, topic []byte, filename []byte, startAt int64, totalSize int64) error {
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
	vol := q.volumes[len(q.volumes)-1]

	files, err := ioutil.ReadDir(filepath.Join(vol, string(topic)))
	if err != nil {
		return 0, 0, err
	}
	if len(files) == 0 {
		return 0, 0, os.ErrNotExist
	}
	var min, max int64
	min = math.MaxInt64
	var maxName string
	for j := range files {
		if !strings.HasSuffix(files[j].Name(), ".dat") {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(files[j].Name(), ".dat"), 10, 64)
		if err != nil {
			continue
		}
		if int64(n) > max {
			max = int64(n)
			maxName = files[j].Name()
		}
		if int64(n) < min {
			min = int64(n)
		}
	}

	if maxName != "" {
		dataFile, err := os.OpenFile(filepath.Join(vol, string(topic), maxName), os.O_RDONLY, 0666)
		if err != nil {
			return 0, 0, err
		}

		info, err := dataFile.Stat()
		if err == nil {
			max += (info.Size() / 24) - 1
		}
	}

	if min >= max {
		return 0, 0, os.ErrNotExist
	}

	return min, max, nil
}
