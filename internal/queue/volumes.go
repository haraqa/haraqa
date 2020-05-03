package queue

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

func checkForDuplicates(volumes []string) error {
	m := make(map[string]struct{}, len(volumes))
	for i := range volumes {
		if _, ok := m[volumes[i]]; ok {
			return errors.Errorf("found duplicate file %s", volumes[i])
		}
		m[filepath.Clean(volumes[i])] = struct{}{}
	}
	return nil
}

func getVolumeTopics(volume string) map[string]*produceTopic {
	m := make(map[string]*produceTopic)

	filepath.Walk(volume, func(path string, info os.FileInfo, err error) error {
		// handle volume doesn't exist
		if info == nil {
			return nil
		}

		// skip directories
		if info.IsDir() {
			return nil
		}

		// find directories with haraqa files
		if strings.HasSuffix(info.Name(), hrqFileExt) {
			path = filepath.Dir(path)
			path = strings.TrimPrefix(path, volume+string(filepath.Separator))
			m[path] = nil
		}
		return nil
	})
	return m
}

func restore(volumes []string, restorationVolume string) error {
	return filepath.Walk(restorationVolume, func(path string, info os.FileInfo, err error) error {
		// skip root
		if restorationVolume == path {
			return nil
		}

		// make directory if not exists
		if info.IsDir() {
			for i := range volumes {
				vPath := strings.Replace(path, restorationVolume, volumes[i], 1)
				err := os.MkdirAll(vPath, os.ModePerm)
				if err != nil {
					return err
				}
			}
			return nil
		}

		// open file
		src, err := os.Open(path)
		if err != nil {
			return err
		}
		defer src.Close()

		// copy file
		for i := range volumes {
			vPath := strings.Replace(path, restorationVolume, volumes[i], 1)
			dst, err := os.Create(vPath)
			if err != nil {
				return err
			}
			_, err = io.Copy(dst, src)
			dst.Close()
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func findOffset(volumes []string, topic string) int64 {
	dir, err := os.Open(filepath.Join(volumes[len(volumes)-1], topic))
	if err != nil {
		return 0
	}

	// err can be ignored since we're checking for names length
	names, _ := dir.Readdirnames(-1)
	dir.Close()
	if len(names) == 0 {
		return 0
	}
	var max int64
	for i := range names {
		if !strings.HasSuffix(names[i], datFileExt) {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSuffix(names[i], datFileExt), 10, 64)
		if err != nil {
			continue
		}
		if n > max {
			max = n
		}
	}
	return max
}
