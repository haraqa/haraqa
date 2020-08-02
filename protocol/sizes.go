package protocol

import (
	"errors"
	"net/http"
	"net/textproto"
	"strconv"
)

// Headers using Canonical MIME structure
const (
	HeaderErrors    = "X-Errors"
	HeaderSizes     = "X-Sizes"
	HeaderLimit     = "X-Limit"
	HeaderStartTime = "X-Start-Time"
	HeaderEndTime   = "X-End-Time"
	HeaderFileName  = "X-File-Name"
)

const (
	errTopicDoesNotExist  = "topic does not exist"
	errTopicAlreadyExists = "topic already exists"
	errInvalidHeaderSizes = "invalid header: " + HeaderSizes
	errInvalidHeaderLimit = "invalid header: " + HeaderLimit
	errInvalidMessageID   = "invalid message id"
	errInvalidTopic       = "invalid topic"
)

// Errors returned by the Client/Server
var (
	ErrTopicDoesNotExist  = errors.New(errTopicDoesNotExist)
	ErrTopicAlreadyExists = errors.New(errTopicAlreadyExists)
	ErrInvalidHeaderSizes = errors.New(errInvalidHeaderSizes)
	ErrInvalidHeaderLimit = errors.New(errInvalidHeaderLimit)
	ErrInvalidMessageID   = errors.New(errInvalidMessageID)
	ErrInvalidTopic       = errors.New(errInvalidTopic)
)

func SetError(w http.ResponseWriter, err error) {
	if err == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	err = errors.Unwrap(err)
	h := w.Header()
	h[HeaderErrors] = []string{err.Error()}
	switch err {
	case ErrTopicDoesNotExist, ErrTopicAlreadyExists:
		w.WriteHeader(http.StatusPreconditionFailed)
	case ErrInvalidHeaderSizes, ErrInvalidHeaderLimit, ErrInvalidMessageID, ErrInvalidTopic:
		w.WriteHeader(http.StatusBadRequest)
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}
	return
}

func ReadErrors(header http.Header) error {
	errs := textproto.MIMEHeader(header)[HeaderErrors]
	if len(errs) == 0 {
		return nil
	}
	for _, err := range errs {
		switch err {
		case "":
			continue
		case errTopicDoesNotExist:
			return ErrTopicDoesNotExist
		case errTopicAlreadyExists:
			return ErrTopicAlreadyExists
		case errInvalidHeaderSizes:
			return ErrInvalidHeaderSizes
		case errInvalidHeaderLimit:
			return ErrInvalidHeaderLimit
		case errInvalidMessageID:
			return ErrInvalidMessageID
		case errInvalidTopic:
			return ErrInvalidTopic
		default:
			return errors.New(err)
		}
	}
	return nil
}

func GetTopic(vars map[string]string) (string, error) {
	topic, _ := vars["topic"]
	if topic == "" {
		return "", ErrInvalidTopic
	}
	return topic, nil
}

func ReadSizes(header http.Header) ([]int64, error) {
	sizes := textproto.MIMEHeader(header)[HeaderSizes]
	if len(sizes) == 0 {
		return nil, ErrInvalidHeaderSizes
	}
	var err error
	msgSizes := make([]int64, len(sizes))
	for i, size := range sizes {
		msgSizes[i], err = strconv.ParseInt(size, 10, 64)
		if err != nil {
			return nil, ErrInvalidHeaderSizes
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
