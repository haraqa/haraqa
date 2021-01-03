package queue

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/haraqa/haraqa/internal/headers"
)

type Queue struct {
	dirs        []string
	fileCache   *sync.Map
	groupCache  *sync.Map
	baseIDCache *sync.Map
	maxEntries  int64
}

func NewQueue(dirs []string, cache bool, maxEntriesPerFile int64) (*Queue, error) {
	q := &Queue{
		dirs:       dirs,
		maxEntries: maxEntriesPerFile,
	}
	if cache {
		q.fileCache = &sync.Map{}
		q.groupCache = &sync.Map{}
		q.baseIDCache = &sync.Map{}
	}
	for _, dir := range q.dirs {
		_ = os.MkdirAll(dir, os.ModePerm)
	}
	return q, nil
}

func (q *Queue) ClearCache(before time.Time) {
	q.fileCache.Range(func(key, value interface{}) bool {
		f, ok := value.(*File)
		if !ok && f == nil {
			q.fileCache.Delete(key)
			return true
		}
		before = before.UTC()
		if !before.IsZero() && f.used.After(before) {
			return true
		}

		q.fileCache.Delete(key)
		f.Close()

		return true
	})
}

func (q *Queue) RootDir() string {
	return q.dirs[len(q.dirs)-1]
}
func (q *Queue) Close() error {
	if q.fileCache != nil {
		q.fileCache.Range(func(key, value interface{}) bool {
			f, _ := value.(*File)
			q.fileCache.Delete(key)
			_ = f.Close()
			return true
		})
	}

	return nil
}

func (q *Queue) GetTopicOwner(topic string) (string, error) {
	return "", nil
}
func (q *Queue) ListTopics(prefix, suffix, regex string) ([]string, error) {
	var rgx *regexp.Regexp
	var err error
	if regex != "" && regex != ".*" {
		rgx, err = regexp.Compile(regex)
		if err != nil {
			return nil, err
		}
	}
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
		if prefix != "" && !strings.HasPrefix(name, prefix) {
			continue
		}
		if suffix != "" && !strings.HasSuffix(name, suffix) {
			continue
		}
		if rgx != nil && !rgx.MatchString(name) {
			continue
		}
		names[i] = name
		i++
	}
	return names[:i], nil
}
func (q *Queue) CreateTopic(topic string) error {
	for _, dir := range q.dirs {
		err := os.Mkdir(filepath.Join(dir, topic), os.ModePerm)
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
	for _, dir := range q.dirs {
		err := os.Remove(filepath.Join(dir, topic))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}
func (q *Queue) ModifyTopic(topic string, request headers.ModifyRequest) (*headers.TopicInfo, error) {
	// TODO: truncate topic
	return nil, nil
}

func (q *Queue) Produce(topic string, msgSizes []int64, timestamp uint64, r io.Reader) error {
	if len(msgSizes) == 0 {
		return nil
	}
	baseID, err := q.getLatestBaseID(topic)
	if err != nil {
		return err
	}
	return q.produce(topic, msgSizes, timestamp, r, baseID)
}

func (q *Queue) produce(topic string, msgSizes []int64, timestamp uint64, r io.Reader, baseID int64) error {
	var (
		f   *File
		err error
		key string
	)
	if q.fileCache != nil {
		key = filepath.Join(q.RootDir(), topic, formatName(baseID))
		v, found := q.fileCache.Load(key)
		if found {
			f, _ = v.(*File)
		}
	}

	if f == nil {
		f, err = OpenFile(q.dirs, topic, baseID)
		if os.IsNotExist(err) {
			f, err = CreateFile(q.dirs, topic, baseID, q.maxEntries)
			if os.IsNotExist(err) {
				return headers.ErrTopicDoesNotExist
			}
		}
		if err != nil {
			return err
		}
		if q.fileCache == nil {
			defer f.Close()
		} else {
			q.fileCache.Store(key, f)
		}
	}

	n, err := f.WriteMessages(timestamp, msgSizes, r)
	for errors.Is(err, ErrFileClosed) {
		n, err = f.WriteMessages(timestamp, msgSizes, r)
	}
	if err != nil {
		return err
	}

	if n < len(msgSizes) {
		numEntries, err := f.NumEntries()
		if err != nil {
			return err
		}
		maxEntries, err := f.MaxEntries()
		if err != nil {
			return err
		}
		if numEntries == maxEntries {
			baseID += maxEntries
			if q.baseIDCache != nil {
				q.baseIDCache.Store(topic, baseID)
			}
		}
		return q.produce(topic, msgSizes[n:], timestamp, r, baseID)
	}
	return nil
}

func (q *Queue) Consume(group, topic string, id int64, limit int64, w http.ResponseWriter) (int, error) {
	id = q.getGroupOffsetID(group, topic, id)
	filename, baseID, err := q.getBaseID(topic, id)
	if err != nil {
		return 0, err
	}
	path := filepath.Join(q.RootDir(), topic, filename)
	var f *File
	if q.fileCache != nil {
		v, found := q.fileCache.Load(path)
		if found {
			f, _ = v.(*File)
		}
	}

	if f == nil {
		f, err = OpenFile(q.dirs, topic, baseID)
		if err != nil {
			return 0, err
		}
		if q.fileCache == nil {
			defer f.Close()
		} else {
			q.fileCache.Store(path, f)
		}
	}

	meta, err := f.ReadMeta(id, limit)
	if err != nil {
		return 0, err
	}
	if meta == nil {
		return 0, nil
	}

	wHeader := w.Header()
	wHeader[headers.HeaderStartTime] = []string{meta.startTime.Format(time.ANSIC)}
	wHeader[headers.HeaderEndTime] = []string{meta.endTime.Format(time.ANSIC)}
	wHeader[headers.HeaderFileName] = []string{path}
	wHeader[headers.ContentType] = []string{"application/octet-stream"}
	wHeader[headers.LastModified] = []string{meta.endTime.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")}
	headers.SetSizes(meta.sizes, wHeader)

	var rs io.ReadSeeker = f
	if q.fileCache != nil {
		// lock for the seek operation
		f.mux.Lock()
		defer f.mux.Unlock()
		if f.isClosed {
			tmp, err := os.Open(path)
			if err != nil {
				return 0, err
			}
			defer tmp.Close()
			rs = tmp
		}
	}
	_, err = rs.Seek(meta.startAt, io.SeekStart)
	if err != nil {
		return 0, err
	}
	io.CopyN(w, rs, meta.endAt-meta.startAt)

	//http.ServeContent(w, req, path, meta.endTime, f)
	return len(meta.sizes), nil
}

func (q *Queue) getBaseID(topic string, id int64) (string, int64, error) {
	checkID := id - id%q.maxEntries
	filename := formatName(checkID)
	_, err := os.Stat(filepath.Join(q.RootDir(), topic, filename))
	if err == nil {
		return filename, checkID, nil
	}
	dir, err := os.Open(filepath.Join(q.RootDir(), topic))
	if err != nil {
		return "", 0, err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return "", 0, err
	}
	if len(names) == 0 {
		return formatName(0), 0, err
	}
	sort.Strings(names)
	baseID := int64(0)
	for _, name := range names {
		parsed, err := strconv.ParseInt(name, 10, 64)
		if err != nil {
			continue
		}
		if id < parsed {
			break
		}
		filename = name
		baseID = parsed
	}
	return filename, baseID, nil
}

func (q *Queue) getLatestBaseID(topic string) (int64, error) {
	if q.baseIDCache != nil {
		v, found := q.baseIDCache.Load(topic)
		if found {
			return v.(int64), nil
		}
	}

	dir, err := os.Open(filepath.Join(q.RootDir(), topic))
	if err != nil {
		return 0, err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return 0, err
	}
	if len(names) == 0 {
		if q.baseIDCache != nil {
			q.baseIDCache.Store(topic, int64(0))
		}
		return 0, nil
	}
	sort.Strings(names)
	baseID, err := strconv.ParseInt(names[len(names)-1], 10, 64)
	if err != nil {
		return 0, err
	}
	if q.baseIDCache != nil {
		q.baseIDCache.Store(topic, baseID)
	}
	return baseID, nil
}

func (q *Queue) getGroupOffsetID(group, topic string, id int64) int64 {
	if group == "" || id > 0 {
		return id
	}
	// TODO: LOOKUP consumer group offset
	if q.groupCache != nil {
		v, found := q.groupCache.Load(group + ":" + topic)
		if cachedID, ok := v.(int64); found && ok {
			return cachedID
		}
	}

	return id
}

func (q *Queue) SetConsumerOffset(group, topic string, id int64) error {
	// TODO: set consumer offset
	if q.groupCache != nil {
		q.groupCache.Store(group+":"+topic, id)
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
