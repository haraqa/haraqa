package queue

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/pkg/errors"

	"github.com/haraqa/haraqa/internal/headers"
)

func (q *Queue) GetTopicOwner(topic string) (string, error) {
	return "", nil
}

func (q *Queue) ListTopics(regex *regexp.Regexp) ([]string, error) {
	var err error
	d, err := os.Open(q.RootDir())
	if err != nil {
		return nil, err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	var i int
	for _, name := range names {
		if regex != nil && !regex.MatchString(name) {
			continue
		}
		names[i] = name
		i++
	}
	return names[:i], nil
}

func (q *Queue) CreateTopic(topic string) error {
	if topic == "" || topic == string(filepath.Separator) || strings.Contains(topic, "..") {
		return errors.New("invalid topic")
	}
	for _, dir := range q.dirs {
		err := os.Mkdir(dir+string(filepath.Separator)+topic, os.ModePerm)
		if os.IsExist(err) {
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) DeleteTopic(topic string) error {
	if topic == "" || topic == string(filepath.Separator) || strings.Contains(topic, "..") {
		return errors.New("invalid topic")
	}
	var errs []error
	for _, dir := range q.dirs {
		errs = append(errs, os.RemoveAll(dir+string(filepath.Separator)+topic))
	}
	return firstError(errs)
}

func (q *Queue) ModifyTopic(topic string, request headers.ModifyRequest) (*headers.TopicInfo, error) {
	d, err := os.Open(q.RootDir() + string(filepath.Separator) + topic)
	if err != nil {
		return nil, err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return &headers.TopicInfo{}, err
	}

	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	var trunc string
	if request.Truncate > 0 {
		trunc = formatName(request.Truncate)
	}

	var idx int
	for idx = range names {
		if trunc != "" && trunc >= names[idx] {
			break
		}
		if !request.Before.IsZero() {
			stat, err := os.Stat(q.RootDir() + string(filepath.Separator) + topic + string(filepath.Separator) + names[idx])
			if err != nil {
				return nil, err
			}
			modTime := stat.ModTime()
			if modTime.Before(request.Before) || modTime.Equal(request.Before) {
				break
			}
		}
	}

	var errs []error
	for _, name := range names[idx+1:] {
		for _, dir := range q.dirs {
			errs = append(errs, os.RemoveAll(dir+string(filepath.Separator)+topic+string(filepath.Separator)+name))
		}
	}

	f, err := os.Open(q.RootDir() + string(filepath.Separator) + topic + string(filepath.Separator) + names[0])
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var info [infoSize]byte
	if _, err = f.ReadAt(info[:], 0); err != nil {
		return nil, err
	}

	baseID := int64(binary.LittleEndian.Uint64(info[:8]))
	numEntries := int64(binary.LittleEndian.Uint64(info[16:24]))
	min := baseID
	max := baseID + numEntries

	if idx != 0 {
		minF, err := os.Open(q.RootDir() + string(filepath.Separator) + topic + string(filepath.Separator) + names[idx])
		if err != nil {
			return nil, err
		}
		defer minF.Close()

		var info [infoSize]byte
		if _, err = minF.ReadAt(info[:], 0); err != nil {
			return nil, err
		}
		min = int64(binary.LittleEndian.Uint64(info[:8]))
	}

	return &headers.TopicInfo{
		MinOffset: min,
		MaxOffset: max,
	}, firstError(errs)
}
