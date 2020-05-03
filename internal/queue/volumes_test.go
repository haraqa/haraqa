package queue

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGetVolumeTopics(t *testing.T) {
	m := getVolumeTopics("a_volume_that_doesnt_exist")
	if m == nil {
		t.Fatal(m)
	}
	if len(m) != 0 {
		t.Fatal(m)
	}
}

func TestFindOffset(t *testing.T) {
	volume, topic := ".haraqa-offsets", "find_offset_test"
	dirName := filepath.Join(volume, topic)
	err := os.MkdirAll(dirName, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dirName)

	// empty topic
	offset := findOffset([]string{volume}, topic)
	if offset != 0 {
		t.Fatal(offset)
	}

	// invalid dat name
	f, err := os.Create(filepath.Join(dirName, "nonint.dat"))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	// valid dat names
	f, err = os.Create(filepath.Join(dirName, formatFilename(200)+".dat"))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	f, err = os.Create(filepath.Join(dirName, formatFilename(10)+".dat"))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	offset = findOffset([]string{volume}, topic)
	if offset != 200 {
		t.Fatal(offset)
	}
}
