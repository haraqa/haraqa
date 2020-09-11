package filequeue

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/haraqa/haraqa/internal/headers"
	"github.com/pkg/errors"
)

func (q *FileQueue) ModifyTopic(topic string, request headers.ModifyRequest) (*headers.TopicInfo, error) {
	if topic == "" || request.Truncate == 0 {
		return nil, nil
	}
	topicPath := filepath.Join(q.rootDirNames[len(q.rootDirNames)-1], topic)
	latest, err := getLatestDat(topicPath)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open latest dat file for %q", topic)
	}

	topicInfo := &headers.TopicInfo{}
	err = filepath.Walk(topicPath, func(path string, info os.FileInfo, err error) error {
		// ignore directories
		if info.IsDir() {
			return nil
		}

		// remove all but latest if truncate is negative
		if request.Truncate < 0 && !strings.HasPrefix(info.Name(), latest) {
			return os.Remove(path)
		}

		// remove all before modtime
		if !request.Before.IsZero() && info.ModTime().Before(request.Before) {
			return os.Remove(path)
		}

		// ignore everything but dat files
		if strings.ContainsRune(info.Name(), '.') {
			return nil
		}

		// remove if file is completely before the truncate point
		base, err := strconv.ParseInt(info.Name(), 10, 64)
		if err != nil {
			return err
		}
		datSize := info.Size() / datEntryLength
		if request.Truncate > 0 && base+datSize < request.Truncate {
			if err = os.Remove(path); err != nil {
				return err
			}
			return os.Remove(path + ".log")
		}

		// check if this is the lowest point
		if request.Truncate < 0 || base <= request.Truncate {
			topicInfo.MinOffset = base
			topicInfo.MaxOffset = base + datSize
		}

		// assign the max offset
		if base+datSize > topicInfo.MaxOffset {
			topicInfo.MaxOffset = base + datSize
		}

		return nil
	})
	if err != nil {
		return nil, errors.Wrapf(err, "unable to modify topic %q", topic)
	}

	return topicInfo, nil
}
