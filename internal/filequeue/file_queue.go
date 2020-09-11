package filequeue

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/haraqa/haraqa/internal/headers"
	"github.com/pkg/errors"
)

type FileQueue struct {
	rootDirNames []string
	rootDirs     []*os.File
	max          int64
	produceLocks sync.Map
	produceCache cache
	consumeCache cache
}

type cache interface {
	Load(key interface{}) (interface{}, bool)
	Store(key, value interface{})
	Delete(key interface{})
}

func New(cacheFiles bool, maxEntries int64, dirs ...string) (*FileQueue, error) {
	if len(dirs) == 0 {
		return nil, errors.New("at least one directory must be given")
	}

	dirNames := make([]string, 0, len(dirs))
	dirFiles := make([]*os.File, 0, len(dirs))

	for _, dir := range dirs {
		dir = filepath.Clean(dir)

		f, err := osOpen(dir)
		if os.IsNotExist(err) {
			err = osMkdir(dir, os.ModePerm)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to create queue directory %q", dir)
			}
			f, err = osOpen(dir)
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

	q := &FileQueue{
		rootDirNames: dirNames,
		rootDirs:     dirFiles,
		max:          maxEntries,
	}
	if cacheFiles {
		q.produceCache = &sync.Map{}
		q.consumeCache = &sync.Map{}
	}
	return q, nil
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
	var names []string
	rootDir := q.rootDirNames[len(q.rootDirNames)-1]
	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			return nil
		}
		if path == rootDir {
			return nil
		}
		path = filepath.ToSlash(strings.TrimPrefix(path, rootDir+string(filepath.Separator)))
		names = append(names, path)
		return nil
	})
	return names, err
}

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

func (q *FileQueue) DeleteTopic(topic string) error {
	for _, name := range q.rootDirNames {
		os.RemoveAll(filepath.Join(name, topic))
	}
	return nil
}

func formatName(baseID int64) string {
	return fmt.Sprintf("%016d", baseID)
}
