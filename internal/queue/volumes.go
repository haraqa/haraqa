package queue

import (
	"io"
	"os"
	"path/filepath"
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
