package queue

import "io"

// go:generate mockgen -package queue -destination handle_consume_mocks_test.go io ReadSeeker
// go:generate goimports -w handle_consume_mocks_test.go

type readSeekCloser struct {
	io.ReadSeeker
}

func (rsc readSeekCloser) Close() error { return nil }
