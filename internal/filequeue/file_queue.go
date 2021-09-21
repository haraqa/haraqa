package filequeue

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"github.com/haraqa/haraqa/internal/headers"
)

// FileQueue implements the haraqa queue by storing messages in log files, under topic based directories
type FileQueue struct {
	rootDirNames     []string
	max              int64
	produceLocks     *sync.Map
	produceCache     *sync.Map
	consumeNameCache *sync.Map
}

// New creates a new FileQueue
func New(cacheFiles bool, maxEntries int64, dirs ...string) (*FileQueue, error) {
	if len(dirs) == 0 {
		return nil, errors.New("at least one directory must be given")
	}

	dirNames := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		dir = filepath.Clean(dir)
		info, err := os.Stat(dir)
		if os.IsNotExist(err) {
			err = osMkdir(dir, os.ModePerm)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to create queue directory %q", dir)
			}
			info, err = os.Stat(dir)
		}
		if err != nil {
			return nil, errors.Wrapf(err, "unable to stat queue directory %q", dir)
		}
		if !info.IsDir() {
			return nil, errors.Errorf("path %q is not a directory", dir)
		}

		dirNames = append(dirNames, dir)
	}

	q := &FileQueue{
		rootDirNames: dirNames,
		max:          maxEntries,
		produceLocks: &sync.Map{},
	}
	if cacheFiles {
		q.produceCache = &sync.Map{}
		q.consumeNameCache = &sync.Map{}
	}
	return q, nil
}

// Close closes the queue cached files
func (q *FileQueue) Close() error {
	if q.produceCache != nil {
		q.produceCache.Range(func(key, value interface{}) bool {
			lock, _ := q.produceLocks.Load(key)
			if l, ok := lock.(*sync.Mutex); ok {
				l.Lock()
				defer l.Unlock()
			}
			v, ok := value.(*cacheableProduceFile)
			if ok {
				for _, f := range v.Logs {
					_ = f.Close()
				}
				for _, f := range v.Dats {
					_ = f.Close()
				}
			}
			return true
		})
	}
	return nil
}

// RootDir returns the path to the haraqa queue root directory. This is used to serve the raw files
func (q *FileQueue) RootDir() string {
	return q.rootDirNames[len(q.rootDirNames)-1]
}

// ListTopics returns all of the topic names in the queue
func (q *FileQueue) ListTopics(regex *regexp.Regexp) ([]string, error) {
	var names []string
	rootDir := q.rootDirNames[len(q.rootDirNames)-1]
	err := fs.WalkDir(os.DirFS("."), rootDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return errors.Wrapf(err, "unable to walk directory %q to list topics", rootDir)
		}
		if !d.IsDir() {
			return nil
		}
		if strings.EqualFold(path, rootDir) {
			return nil
		}
		path = filepath.ToSlash(strings.TrimPrefix(path, rootDir+string(filepath.Separator)))

		if regex != nil && !regex.MatchString(path) {
			return nil
		}
		names = append(names, path)
		return nil
	})
	return names, err
}

// CreateTopic creates a new topic if it does not already exist
func (q *FileQueue) CreateTopic(topic string) error {
	topic = strings.TrimSpace(topic)
	topic = strings.TrimSuffix(topic, "/")
	splitTopic := strings.Split(topic, "/")
	for _, name := range q.rootDirNames {
		var err error
		if len(splitTopic) == 1 {
			err = osMkdir(filepath.Join(name, topic), os.ModePerm)
		} else {
			err = osMkdirAll(filepath.Join(name, filepath.Join(splitTopic[:len(splitTopic)-1]...)), os.ModePerm)
			if err != nil {
				return err
			}
			err = osMkdir(filepath.Join(name, filepath.Join(splitTopic...)), os.ModePerm)
		}
		if os.IsExist(err) {
			return headers.ErrTopicAlreadyExists
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteTopic deletes the topic and any nested topic within
func (q *FileQueue) DeleteTopic(topic string) error {
	for _, name := range q.rootDirNames {
		os.RemoveAll(filepath.Join(name, topic))
	}
	if q.consumeNameCache != nil {
		q.consumeNameCache.Delete(topic)
	}
	if q.produceCache != nil {
		q.produceCache.Delete(topic)
	}
	if q.produceLocks != nil {
		q.produceLocks.Delete(topic)
	}

	return nil
}

func formatName(baseID int64) string {
	const defaultName = "0000000000000000"

	if baseID <= 0 {
		return defaultName
	}
	v := strconv.FormatInt(baseID, 10)
	if len(v) < 16 {
		v = defaultName[len(v):] + v
	}
	return v
}
