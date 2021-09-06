package queue

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/haraqa/haraqa/internal/headers"
)

func (q *Queue) Consume(group, topic string, id int64, limit int64, w http.ResponseWriter) (int, error) {
	id = q.getGroupOffsetID(group, topic, id)
	filename, baseID, err := q.getBaseID(topic, id)
	if err != nil {
		return 0, err
	}
	path := q.RootDir() + string(filepath.Separator) + topic + string(filepath.Separator) + filename
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
	if len(meta.sizes) == 0 {
		return 0, nil
	}

	wHeader := w.Header()
	wHeader[headers.HeaderFileName] = []string{topic + "/" + filename}
	wHeader[headers.ContentType] = []string{"application/octet-stream"}
	headers.SetSizes(meta.sizes, wHeader)

	// TODO: evaluate if we need timestamps in response message
	//wHeader[headers.HeaderStartTime] = []string{meta.startTime.Format(time.ANSIC)}
	//wHeader[headers.HeaderEndTime] = []string{meta.endTime.Format(time.ANSIC)}
	//wHeader[headers.LastModified] = []string{meta.endTime.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")}

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
	if _, err = io.CopyN(w, rs, meta.endAt-meta.startAt); err != nil {
		return 0, err
	}

	//http.ServeContent(w, req, path, meta.endTime, f)
	return len(meta.sizes), nil
}

func (q *Queue) getBaseID(topic string, id int64) (string, int64, error) {
	checkID := id - id%q.maxEntries
	filename := formatName(checkID)
	_, err := os.Stat(q.RootDir() + string(filepath.Separator) + topic + string(filepath.Separator) + filename)
	if err == nil && id >= 0 {
		return filename, checkID, nil
	}
	dir, err := os.Open(q.RootDir() + string(filepath.Separator) + topic)
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
		if id >= 0 && id < parsed {
			break
		}
		filename = name
		baseID = parsed
	}
	return filename, baseID, nil
}

/*
func (q *Queue) getBaseID(topic string, id int64) (string, int64, error) {
	checkID := id - id%q.maxEntries
	filename := formatName(checkID)
	_, err := fs.Stat(os.DirFS(q.RootDir()), topic+string(filepath.Separator)+filename)
	if err == nil {
		return filename, checkID, nil
	}
	entries, err := fs.ReadDir(os.DirFS(q.RootDir()), topic) //.Readdirnames(-1)
	if err != nil {
		return "", 0, err
	}
	if len(entries) == 0 {
		return formatName(0), 0, err
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})
	baseID := int64(0)
	for i := range entries {
		parsed, err := strconv.ParseInt(entries[i].Name(), 10, 64)
		if err != nil {
			continue
		}
		if id < parsed {
			break
		}
		filename = entries[i].Name()
		baseID = parsed
	}
	return filename, baseID, nil
}
*/

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
