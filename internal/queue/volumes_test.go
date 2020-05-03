package queue

import (
	"os"
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
	err := os.MkdirAll(".haraqa-offsets/find_offset_test", os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(".haraqa-offsets/find_offset_test")

	// empty topic
	offset := findOffset([]string{".haraqa"}, "find_offset_test")
	if offset != 0 {
		t.Fatal(offset)
	}

	// invalid dat name
	f, err := os.Create(".haraqa-offsets/find_offset_test/nonint.dat")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	// valid dat names
	f, err = os.Create(".haraqa-offsets/find_offset_test/" + formatFilename(200) + ".dat")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	f, err = os.Create(".haraqa-offsets/find_offset_test/" + formatFilename(10) + ".dat")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	offset = findOffset([]string{".haraqa"}, "find_offset_test")
	if offset != 200 {
		t.Fatal(offset)
	}
}
