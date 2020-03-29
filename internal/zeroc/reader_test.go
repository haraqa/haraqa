package zeroc

import (
	"os"
	"testing"
)

func TestReader(t *testing.T) {
	in, err := os.Create("in.test")
	if err != nil {
		t.Fatal(err)
	}
	_, err = in.Write([]byte("input"))
	if err != nil {
		t.Fatal(err)
	}
	out, err := os.Create("out.test")
	if err != nil {
		t.Fatal(err)
	}
	r := NewReader(in, 0, 5)

	n, err := r.WriteTo(out)
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatal(n)
	}

	in.Close()
	_, err = r.WriteTo(out)
	if err == nil {
		t.Fatal("expected error")
	}

	_, err = r.WriteTo(nil)
	if err == nil {
		t.Fatal("expected error")
	}
}
