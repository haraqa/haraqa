package filequeue

import (
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/haraqa/haraqa/internal/headers"
	"github.com/pkg/errors"
)

// ModifyTopic updates the topic to truncate/remove messages and return the topic offset info
func (q *FileQueue) ModifyTopic(topic string, request headers.ModifyRequest) (*headers.TopicInfo, error) {
	if topic == "" {
		return nil, nil
	}
	topicPath := filepath.Join(q.rootDirNames[len(q.rootDirNames)-1], topic)
	latest, err := getLatestDat(topicPath)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open latest dat file for %q", topic)
	}

	topicInfo := &headers.TopicInfo{}
	err = fs.WalkDir(os.DirFS("."), topicPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				err = nil
			}
			return err
		}
		if d.IsDir() || strings.ContainsRune(d.Name(), '.') {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		return truncateTopic(request, topicInfo, latest, path, info)
	})
	if err != nil {
		return nil, errors.Wrapf(err, "unable to modify topic %q", topic)
	}

	return topicInfo, nil
}

func truncateTopic(request headers.ModifyRequest, topicInfo *headers.TopicInfo, latest string, path string, info fs.FileInfo) error {
	// remove all but latest if truncate is negative
	if request.Truncate < 0 && !strings.HasPrefix(info.Name(), latest) {
		return errors.Wrapf(os.Remove(path), "unable to remove truncated file %s", path)
	}

	// remove all before modtime
	if !request.Before.IsZero() && info.ModTime().Before(request.Before) {
		return errors.Wrapf(os.Remove(path), "unable to remove timed out file %s", path)
	}

	// remove if file is completely before the truncate point
	base, err := strconv.ParseInt(info.Name(), 10, 64)
	if err != nil {
		return errors.Wrapf(os.Remove(path), "unable to remove unparsable file %s", path)
	}
	datSize := info.Size() / datEntryLength
	if request.Truncate > 0 && base+datSize < request.Truncate {
		if err = os.Remove(path); err != nil {
			return errors.Wrapf(os.Remove(path), "unable to remove file %s", path)
		}
		return errors.Wrapf(os.Remove(path+".log"), "unable to remove file %s", path)
	}

	// check if this is the lowest point
	if request.Truncate < 0 || base <= request.Truncate {
		topicInfo.MinOffset = base
		topicInfo.MaxOffset = base + datSize - 1
	}

	// assign the max offset
	if base+datSize > topicInfo.MaxOffset {
		topicInfo.MaxOffset = base + datSize - 1
	}
	return nil
}
