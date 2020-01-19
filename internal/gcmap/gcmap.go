package gcmap

import (
	"io"
	"sync"
)

func NewMap(channelSize uint64) *Map {
	return &Map{
		m: make(map[string]chan io.Closer),
		d: make(map[string]struct{}),
		s: channelSize,
		l: 1,
	}
}

type Map struct {
	mux sync.Mutex
	m   map[string]chan io.Closer
	d   map[string]struct{}
	s   uint64
	l   int
}

func (m *Map) Get(key []byte) io.Closer {
	ch, ok := m.m[string(key)]
	if !ok {
		return nil
	}
	select {
	case v := <-ch:
		return v
	default:
		return nil
	}
}

func (m *Map) Put(key []byte, val io.Closer) {
	m.mux.Lock()
	delete(m.d, string(key))
	ch, ok := m.m[string(key)]
	if !ok {
		ch = m.newChannel(key)
	}
	select {
	case ch <- val:
	default:
		val.Close()
	}
	m.mux.Unlock()
}

func (m *Map) newChannel(key []byte) chan io.Closer {
	ch := make(chan io.Closer, m.s)
	m.m[string(key)] = ch

	if len(m.m)*2 > m.l {
		m.gc()
		m.l = len(m.m) + 1
	}
	return ch
}

func (m *Map) gc() {
	for key := range m.d {
		delete(m.d, key)
		ch, ok := m.m[key]
		if !ok {
			continue
		}
		delete(m.m, key)
	loop:
		for {
			select {
			case v := <-ch:
				v.Close()
			default:
				break loop
			}
		}
	}

	for key := range m.m {
		m.d[key] = struct{}{}
	}
}
