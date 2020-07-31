package protocol

import (
	"net/http"
	"net/textproto"
	"strconv"
)

// Headers using Canonical MIME structure
const (
	HeaderSizes     = "X-Sizes"
	HeaderBatchSize = "X-Batch-Size"
	HeaderStartTime = "X-Start-Time"
	HeaderEndTime   = "X-End-Time"
	HeaderFileName  = "X-File-Name"
)

func ReadSizes(header http.Header) ([]int64, error) {
	sizes := textproto.MIMEHeader(header)[HeaderSizes]

	var err error
	msgSizes := make([]int64, len(sizes))
	for i, size := range sizes {
		msgSizes[i], err = strconv.ParseInt(size, 10, 64)
		if err != nil {
			return nil, err
		}
	}
	return msgSizes, nil
}

func SetSizes(msgSizes []int64, h http.Header) http.Header {
	sizes := make([]string, len(msgSizes))
	for i := range msgSizes {
		sizes[i] = strconv.FormatInt(msgSizes[i], 10)
	}
	h[HeaderSizes] = sizes
	return h
}
