package filequeue

import "os"

var (
	osMkdir    = os.Mkdir
	osMkdirAll = os.MkdirAll
	osOpen     = os.Open
)
