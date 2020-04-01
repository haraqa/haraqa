package zeroc

import (
	"os"
	"testing"
)

func TestMultiWriter(t *testing.T) {
	f1, err := os.Create("f1.test")
	if err != nil {
		t.Fatal(err)
	}
	f2, err := os.Create("f2.test")
	if err != nil {
		t.Fatal(err)
	}
	f3, err := os.Create("f3.test")
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewMultiWriter(f1, f2, f3)
	if err != nil {
		t.Fatal(err)
	}
	offset := w.SetLimit(100).IncreaseOffset(0).Offset()
	if offset != 0 {
		t.Fatal(offset)
	}

	_, err = w.Write([]byte("example"))
	if err != nil {
		t.Fatal(err)
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	_, err = w.Write([]byte("example"))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestMultiWriterRead(t *testing.T) {
	r1, err := os.Create("r1.test")
	if err != nil {
		t.Fatal(err)
	}
	r2, err := os.Create("r2.test")
	if err != nil {
		t.Fatal(err)
	}
	f1, err := os.Open("f1.test")
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewMultiWriter(r1, r2)
	if err != nil {
		t.Fatal(err)
	}
	n, err := w.SetLimit(5).ReadFrom(f1)
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatal(n)
	}

	err = r1.Close()
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.SetLimit(5).ReadFrom(f1)
	if err == nil {
		t.Fatal("expected error")
	}

	err = f1.Close()
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.SetLimit(5).ReadFrom(f1)
	if err == nil {
		t.Fatal("expected error")
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	_, err = w.SetLimit(5).ReadFrom(nil)
	if err == nil {
		t.Fatal("expected error")
	}
}
