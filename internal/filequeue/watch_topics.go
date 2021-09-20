package filequeue

import (
	"io"
	"path/filepath"
	"strings"

	"github.com/fsnotify/fsnotify"
)

func (q *FileQueue) WatchTopics(topics []string) (written, deleted chan string, closer io.Closer, err error) {
	// setup watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return
	}
	written = make(chan string, len(watcher.Events))
	deleted = make(chan string, len(watcher.Events))
	closer = watcher
	defer func() {
		if err != nil {
			_ = closer.Close()
		}
	}()
	rootDir := q.RootDir()
	for _, topic := range topics {
		err = watcher.Add(rootDir + string(filepath.Separator) + topic)
		if err != nil {
			return
		}
	}
	go func() {
		for event := range watcher.Events {
			if event.Op == fsnotify.Write && !strings.HasSuffix(event.Name, ".log") {
				topic := strings.TrimPrefix(filepath.Dir(event.Name), rootDir+string(filepath.Separator))
				written <- topic
			} else if event.Op == fsnotify.Remove {
				topic := strings.TrimPrefix(filepath.Dir(event.Name), rootDir+string(filepath.Separator))
				_ = watcher.Remove(filepath.Dir(event.Name))
				deleted <- topic
			}
		}
	}()
	return
}
