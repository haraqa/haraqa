package filequeue

import (
	"fmt"
	"os"
	"path/filepath"
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
}

func New(dirs ...string) (*FileQueue, error) {
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
		max:          5000,
		produceCache: &sync.Map{},
		consumeCache: &sync.Map{},
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

func (q *FileQueue) ModifyTopic(topic string, request headers.ModifyRequest) (*headers.TopicInfo, error) {
	return nil, nil
}

func formatName(baseID int64) string {
	return fmt.Sprintf("%016d", baseID)
}
