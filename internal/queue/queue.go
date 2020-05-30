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
	"time"

	"github.com/haraqa/haraqa/internal/gcmap"
	"github.com/haraqa/haraqa/internal/protocol"
	"github.com/pkg/errors"
)

//go:generate mockgen -source queue.go -destination ../mocks/queue.go -package mocks

// Queue is the interface for dealing with topics and messages in the haraqa queue
type Queue interface {
	CreateTopic(topic []byte) error
	DeleteTopic(topic []byte) error
	TruncateTopic(topic []byte, offset int64, before time.Time) error
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
		produceFileSets: make(map[string]*produceFileSet),
		consumeTopics:   gcmap.NewMap(consumePoolSize),
		volumes:         volumes,
		maxEntries:      int64(maxEntries),
	}
	return q, nil
}

type queue struct {
	sync.Mutex
	volumes    []string
	maxEntries int64

	produceLocks    sync.Map
	produceFileSets map[string]*produceFileSet
	produceMapLock  sync.RWMutex

	consumeTopics *gcmap.Map
}

func (q *queue) CreateTopic(topic []byte) error {
	q.Lock()
	defer q.Unlock()

	for _, v := range q.volumes {
		dir, err := os.Open(filepath.Join(v, string(topic)))
		if err == nil {
			dir.Close()
			return protocol.ErrTopicExists
		}

		err = os.MkdirAll(filepath.Join(v, string(topic)), os.ModePerm)
		if err != nil {
			return err
		}
	}

	return nil
}

func (q *queue) DeleteTopic(topic []byte) error {
	q.Lock()
	defer q.Unlock()

	l, ok := q.produceLocks.Load(string(topic))
	if ok {
		l.(*sync.Mutex).Lock()
		q.produceMapLock.Lock()
		delete(q.produceFileSets, string(topic))
		q.produceMapLock.Unlock()
		q.produceLocks.Delete(string(topic))
		l.(*sync.Mutex).Unlock()
	}

	for i := range q.volumes {
		err := os.RemoveAll(filepath.Join(q.volumes[i], string(topic)))
		if err != nil {
			return err
		}
	}

	return nil
}

// TruncateTopic will remove topic files up to the file containing the given offset
func (q *queue) TruncateTopic(topic []byte, offset int64, before time.Time) error {
	root := filepath.Join(q.volumes[len(q.volumes)-1], string(topic))
	deletes := make(map[string]struct{})
	var latestFileName string
	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		// skip root directory
		if path == root {
			return nil
		}

		// skip subdirectories
		if info.IsDir() {
			return filepath.SkipDir
		}

		// skip non dat files
		if !strings.HasSuffix(info.Name(), datFileExt) {
			return nil
		}

		// save the last modified
		if info.Name() > latestFileName {
			latestFileName = info.Name()
		}

		// delete if older than before
		if !before.IsZero() && info.ModTime().Before(before) {
			deletes[info.Name()] = struct{}{}
			return nil
		}

		// if offset is zero return fast
		if offset == 0 {
			return nil
		}

		// if negative offset, delete all but last
		if offset < 0 {
			deletes[info.Name()] = struct{}{}
			return nil
		}

		// delete if less than offset
		o, err := strconv.ParseInt(strings.TrimSuffix(info.Name(), datFileExt), 10, 64)
		if err != nil || o+info.Size()/datumLength < offset {
			deletes[info.Name()] = struct{}{}
			return nil
		}

		return nil
	})
	delete(deletes, latestFileName)

	for _, volume := range q.volumes {
		path := filepath.Join(volume, string(topic))
		for name := range deletes {
			err := os.Remove(filepath.Join(path, name))
			if err != nil {
				return err
			}
			hrq := strings.TrimSuffix(name, datFileExt) + hrqFileExt
			err = os.Remove(filepath.Join(path, hrq))
			if err != nil {
				return err
			}
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
	output := make([][]byte, 0, len(q.produceFileSets))
	v := q.volumes[len(q.volumes)-1]
	filepath.Walk(v, func(path string, info os.FileInfo, err error) error {
		if path == v {
			return nil
		}
		if !info.IsDir() {
			return nil
		}

		topic := strings.TrimPrefix(path, v+string(filepath.Separator))

		if prefix != "" && !strings.HasPrefix(topic, prefix) {
			return nil
		}
		if suffix != "" && !strings.HasSuffix(topic, suffix) {
			return nil
		}
		if r != nil && !r.MatchString(topic) {
			return nil
		}

		output = append(output, []byte(topic))
		return nil
	})

	return output, nil
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
		info, err := os.Stat(filepath.Join(path, maxName))
		if err == nil {
			max += (info.Size() / 24)
		}
	}

	if min >= max {
		return 0, 0, os.ErrNotExist
	}

	return min, max, nil
}
