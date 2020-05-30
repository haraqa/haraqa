package queue

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/haraqa/haraqa/internal/protocol"
	"github.com/haraqa/haraqa/internal/zeroc"
	"github.com/pkg/errors"
)

type produceFileSet struct {
	sync.Mutex
	relOffset    int64
	globalOffset int64
	dat          *zeroc.MultiWriter
	messages     *zeroc.MultiWriter
}

func (q *queue) Produce(tcpConn io.Reader, topic []byte, msgSizes []int64) error {
	if len(topic) == 0 {
		return protocol.ErrTopicDoesNotExist
	}

	// lock this topic
	l, ok := q.produceLocks.Load(string(topic))
	if !ok {
		l, _ = q.produceLocks.LoadOrStore(string(topic), new(sync.Mutex))
	}
	l.(*sync.Mutex).Lock()
	defer l.(*sync.Mutex).Unlock()

	// get the multiwriters for the topic
	pfs, err := q.getProduceFileSet(topic, msgSizes)
	if err != nil {
		return err
	}

	// write messages to file
	n, err := pfs.messages.SetLimit(sum(msgSizes)).ReadFrom(tcpConn)
	if err != nil {
		return errors.Wrap(err, "issue writing to file")
	}

	// write messages metadata to dat
	offset := pfs.globalOffset
	startAt := pfs.messages.Offset()
	err = writeDat(pfs.dat, offset, startAt, msgSizes)
	if err != nil {
		return err
	}

	// update topic struct
	pfs.messages.IncreaseOffset(n)
	pfs.relOffset += int64(len(msgSizes))
	pfs.globalOffset += int64(len(msgSizes))
	return nil
}

func sum(s []int64) int64 {
	var out int64
	for _, v := range s {
		out += v
	}
	return out
}

func (q *queue) getProduceFileSet(topic []byte, msgSizes []int64) (*produceFileSet, error) {
	var err error

	q.produceMapLock.RLock()
	pfs, ok := q.produceFileSets[string(topic)]
	q.produceMapLock.RUnlock()

	switch {
	case !ok:
		// attempt to open files for the topic
		pfs, err = q.openProduceFileSet(topic, msgSizes)
		if err != nil {
			return nil, err
		}
	case pfs.relOffset+int64(len(msgSizes)) > q.maxEntries:
		// if number of messages will push the file size over the edge, make a new file
		pfs.dat.Close()
		pfs.messages.Close()
		pfs, err = q.newProduceFileSet(topic, pfs.globalOffset)
		if err != nil {
			return nil, err
		}
	default:
		return pfs, nil
	}

	// update map
	q.produceMapLock.Lock()
	q.produceFileSets[string(topic)] = pfs
	q.produceMapLock.Unlock()

	return pfs, nil
}

func (q *queue) openProduceFileSet(topic []byte, msgSizes []int64) (*produceFileSet, error) {
	// check if topic exists
	dir, err := os.Open(filepath.Join(q.volumes[len(q.volumes)-1], string(topic)))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, protocol.ErrTopicDoesNotExist
		}
		return nil, err
	}

	// err can be ignored since we're checking for names length
	names, _ := dir.Readdirnames(-1)
	dir.Close()
	if len(names) == 0 {
		return q.newProduceFileSet(topic, 0)
	}

	var max int64
	var maxName string
	for i := range names {
		if !strings.HasSuffix(names[i], datFileExt) {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSuffix(names[i], datFileExt), 10, 64)
		if err != nil {
			continue
		}
		if n > max {
			max = n
			maxName = names[i]
		}
	}

	// check offset
	dat, err := os.Open(filepath.Join(q.volumes[len(q.volumes)-1], string(topic), maxName))
	if err != nil {
		return nil, err
	}
	stat, err := dat.Stat()
	if err != nil {
		return nil, err
	}
	dat.Close()

	// if the message sizes will exceed the length create a new file
	count := stat.Size() / datumLength
	if count > 0 && count+int64(len(msgSizes)) > q.maxEntries {
		return q.newProduceFileSet(topic, max+count)
	}

	dats := make([]*os.File, 0, len(q.volumes))
	msgs := make([]*os.File, 0, len(q.volumes))
	for _, v := range q.volumes {
		d, m, err := openDatAndMsgs(v, topic, strings.TrimSuffix(maxName, datFileExt), os.O_RDWR)
		if err != nil {
			return nil, err
		}
		dats = append(dats, d)
		msgs = append(msgs, m)
	}

	datMW, err := zeroc.NewMultiWriter(dats...)
	if err != nil {
		return nil, err
	}
	msgMW, err := zeroc.NewMultiWriter(msgs...)
	if err != nil {
		return nil, err
	}
	return &produceFileSet{
		dat:          datMW,
		messages:     msgMW,
		relOffset:    count,
		globalOffset: max + count,
	}, nil
}

func openDatAndMsgs(v string, topic []byte, filename string, tags int) (*os.File, *os.File, error) {

	d, err := os.OpenFile(filepath.Join(v, string(topic), filename+datFileExt), tags, 0666)
	if err != nil {
		return nil, nil, err
	}
	m, err := os.OpenFile(filepath.Join(v, string(topic), filename+hrqFileExt), tags, 0666)
	if err != nil {
		return nil, nil, err
	}
	return d, m, nil
}

func (q *queue) newProduceFileSet(topic []byte, globalOffset int64) (*produceFileSet, error) {
	offsetString := formatFilename(globalOffset)
	dats := make([]*os.File, 0, len(q.volumes))
	msgs := make([]*os.File, 0, len(q.volumes))
	for _, v := range q.volumes {
		d, m, err := openDatAndMsgs(v, topic, offsetString, os.O_RDWR|os.O_CREATE|os.O_TRUNC)
		if err != nil {
			return nil, err
		}
		dats = append(dats, d)
		msgs = append(msgs, m)
	}

	datMW, err := zeroc.NewMultiWriter(dats...)
	if err != nil {
		return nil, err
	}
	msgMW, err := zeroc.NewMultiWriter(msgs...)
	if err != nil {
		return nil, err
	}
	return &produceFileSet{
		dat:          datMW,
		messages:     msgMW,
		relOffset:    0,
		globalOffset: globalOffset,
	}, nil
}
