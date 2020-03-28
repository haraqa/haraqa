package gcmap

import "testing"

type closer struct {
	err error
}

func (c *closer) Close() error {
	return c.err
}

func TestMap(t *testing.T) {
	m := NewMap(1)
	key := []byte("mykey")
	v := m.Get(key)
	if v != nil {
		t.Fatal(v)
	}

	m.Put([]byte("myotherkey"), &closer{err: nil})
	m.Put(key, &closer{err: nil})
	m.Put(key, &closer{err: nil})
	if m.Get(key) == nil {
		t.Fatal("expected value")
	}

	v = m.Get(key)
	if v != nil {
		t.Fatal(v)
	}
	m.Put(key, &closer{err: nil})
	m.Put(key, &closer{err: nil})

}
