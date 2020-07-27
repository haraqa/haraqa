package protocol

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
)

func ReadSizes(header http.Header) ([]int64, error) {
	xSizes := header.Get("X-SIZES")
	if xSizes == "" {
		return nil, errors.New("Missing required header X-SIZES")
	}

	var err error
	sizes := strings.Split(xSizes, ",")
	msgSizes := make([]int64, len(sizes))
	for i, size := range sizes {
		msgSizes[i], err = strconv.ParseInt(size, 10, 64)
		if err != nil {
			return nil, err
		}
	}
	return msgSizes, nil
}

func SetSizes(msgSizes []int64) (string, string) {
	sizes := make([]string, len(msgSizes))
	for i := range msgSizes {
		sizes[i] = strconv.FormatInt(msgSizes[i], 10)
	}
	return "X-SIZES", strings.Join(sizes, ",")
}
