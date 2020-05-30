package queue

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/haraqa/haraqa/internal/protocol"
	"github.com/haraqa/haraqa/internal/zeroc"
	"github.com/pkg/errors"
)

// ConsumeInfo returns the info required to consume from a specific file. it
// returns the filepath of the file to be read from, the starting point to begin
// reading from, and the number and lengths of the messages to be read
func (q *queue) ConsumeInfo(topic []byte, offset int64, limit int64) ([]byte, int64, []int64, error) {
	dir := q.consumeTopics.Get(topic)
	if dir == nil {
		var err error
		dir, err = os.Open(filepath.Join(q.volumes[len(q.volumes)-1], string(topic)))
		if err != nil {
			if os.IsNotExist(err) {
				err = protocol.ErrTopicDoesNotExist
			}
			return nil, 0, nil, err
		}
	}

	// get a list of all the files
	names, err := dir.(*os.File).Readdirnames(-1)
	if err != nil {
		return nil, 0, nil, err
	}
	q.consumeTopics.Put(topic, dir)

	// find the file which matches the offset
	var baseOffset int64
	for i := range names {
		if !strings.HasSuffix(names[i], datFileExt) {
			continue
		}
		o, err := strconv.ParseInt(strings.TrimSuffix(names[i], datFileExt), 10, 64)
		if err == nil {
			if o > baseOffset && (offset < 0 || o <= offset) {
				baseOffset = o
			}
		}
	}
	filename := filepath.Join(q.volumes[len(q.volumes)-1], string(topic), formatFilename(baseOffset))

	// read from .dat file
	f, err := os.Open(filename + datFileExt)
	if err != nil {
		return nil, 0, nil, err
	}
	startAt, msgSizes, err := readDat(f, offset, limit)
	if err != nil {
		return nil, 0, nil, err
	}

	return []byte(filename + hrqFileExt), startAt, msgSizes, nil
}

func (q *queue) Consume(tcpConn io.Writer, topic []byte, filename []byte, startAt int64, totalSize int64) error {
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
