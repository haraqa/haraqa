package testing

import (
	"os"
	"testing"

	"github.com/haraqa/haraqa/internal/zeroc"
)

func TestMultiWriter(t *testing.T) {
	f1, err := os.Create("example_1.test")
	if err != nil {
		t.Fatal(err)
	}

	f2, err := os.Create("example_2.test")
	if err != nil {
		t.Fatal(err)
	}

	w, err := zeroc.NewMultiWriter(f1, f2)
	if err != nil {
		t.Fatal(err)
	}

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	_, err = writer.Write([]byte("Hello there world, test one two three"))
	if err != nil {
		t.Fatal(err)
	}

	n, err := w.SetLimit(20).ReadFrom(reader)
	if err != nil {
		t.Fatal(err)
	}
	if n != 20 {
		t.Fatal("invalid n", n)
	}

	b := make([]byte, 20)
	_, err = f2.ReadAt(b, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "Hello there world, t" {
		t.Fatal(string(b))
	}
}
