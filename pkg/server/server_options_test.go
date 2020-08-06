package server

import (
	"reflect"
	"testing"
)

func TestServerOptions(t *testing.T) {
	// WithQueue
	{
		s := &Server{}
		err := WithQueue(nil)(s)
		if err.Error() != "queue cannot be nil" {
			t.Fatal(err)
		}

		q := &MockQueue{}
		err = WithQueue(q)(s)
		if err != nil {
			t.Fatal(err)
		}
		if s.q != q {
			t.Fatal(s.q, q)
		}
	}

	// WithDirs
	{
		s := &Server{}
		err := WithDirs()(s)
		if err.Error() != "at least one directory must be given" {
			t.Fatal(err)
		}

		d := []string{"a", "b", "c"}
		err = WithDirs(d...)(s)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(s.dirs, d) {
			t.Fatal(s.dirs, d)
		}
	}

	// WithMetrics
	{
		s := &Server{}
		err := WithMetrics(nil)(s)
		if err.Error() != "metrics cannot be nil" {
			t.Fatal(err)
		}

		m := noOpMetrics{}
		err = WithMetrics(m)(s)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(s.metrics, m) {
			t.Fatal(s.metrics, m)
		}
	}

	// WithDefaultConsumeLimit
	{
		s := &Server{}
		err := WithDefaultConsumeLimit(0)(s)
		if err != nil {
			t.Fatal(err)
		}
		if s.defaultLimit != -1 {
			t.Fatal(s.defaultLimit)
		}

		err = WithDefaultConsumeLimit(-1)(s)
		if err != nil {
			t.Fatal(err)
		}
		if s.defaultLimit != -1 {
			t.Fatal(s.defaultLimit)
		}

		err = WithDefaultConsumeLimit(1024)(s)
		if err != nil {
			t.Fatal(err)
		}
		if s.defaultLimit != 1024 {
			t.Fatal(s.defaultLimit)
		}
	}
}
