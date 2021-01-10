package queue

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/pkg/errors"
)

func TestCreateFile(t *testing.T) {

	if _, err := CreateFile(nil, "topic", 0, 1); err == nil {
		t.Error(err)
	}
	if _, err := CreateFile([]string{"dir"}, "", 0, 1); err == nil {
		t.Error(err)
	}
	if _, err := CreateFile([]string{"dir"}, "topic", -1, 1); err == nil {
		t.Error(err)
	}
	if _, err := CreateFile([]string{"dir"}, "topic", 0, 0); err == nil {
		t.Error(err)
	}
}

func TestOpenFile(t *testing.T) {
	if _, err := OpenFile(nil, "topic", 0); err == nil {
		t.Error(err)
	}
	if _, err := OpenFile([]string{"dir"}, "", 0); err == nil {
		t.Error(err)
	}
	if _, err := OpenFile([]string{"dir"}, "topic", -1); err == nil {
		t.Error(err)
	}
}

func TestInitFile(t *testing.T) {
	var info [infoSize]byte
	if _, err := rand.Read(info[:]); err != nil {
		t.Error(err)
	}

	ws := &mockWS{
		expectedWrites: make(chan []byte, 2),
		writeResponses: make(chan error, 2),
		expectedSeeks:  make(chan int64, 1),
		seekResponses:  make(chan error, 1),
	}
	e := errors.New("example error")

	ws.expectedWrites <- info[:]
	ws.writeResponses <- e
	err := initFile(ws, info, 22)
	if !errors.Is(err, e) {
		t.Error(err, e)
	}

	ws.expectedWrites <- info[:]
	ws.writeResponses <- nil
	ws.expectedSeeks <- infoSize + 22*metaSize - 1
	ws.seekResponses <- e
	err = initFile(ws, info, 22)
	if !errors.Is(err, e) {
		t.Error(err, e)
	}

	ws.expectedWrites <- info[:]
	ws.writeResponses <- nil
	ws.expectedSeeks <- infoSize + 22*metaSize - 1
	ws.seekResponses <- nil
	ws.expectedWrites <- []byte{0}
	ws.writeResponses <- e
	err = initFile(ws, info, 22)
	if !errors.Is(err, e) {
		t.Error(err, e)
	}
}

type mockWS struct {
	expectedWrites chan []byte
	writeResponses chan error
	expectedSeeks  chan int64
	seekResponses  chan error
}

func (ws *mockWS) Write(b []byte) (int, error) {
	select {
	case expect := <-ws.expectedWrites:
		if !bytes.Equal(b, expect) {
			return 0, errors.Errorf("expected %s, got %s", expect, b)
		}
	default:
		return 0, errors.New("unexpected write")
	}

	select {
	case err := <-ws.writeResponses:
		if err == nil {
			return len(b), err
		}
		return 0, err
	default:
		return 0, errors.New("missing write response")
	}
}

func (ws *mockWS) Seek(offset int64, whence int) (int64, error) {
	select {
	case expect := <-ws.expectedSeeks:
		if offset != expect {
			return 0, errors.Errorf("expected %d, got %d", expect, offset)
		}
	default:
		return 0, errors.New("unexpected seek")
	}

	select {
	case err := <-ws.seekResponses:
		if err == nil {
			return offset, err
		}
		return 0, err
	default:
		return 0, errors.New("missing seek response")
	}
}
