package headers

import (
	"net/http"
	"strconv"

	"github.com/pkg/errors"
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
	errInvalidBodyMissing = "invalid body: body cannot be empty"
	errInvalidBodyJSON    = "invalid body: invalid json entry"
	errNoContent          = "no content"
)

// Errors returned by the Client/Server
var (
	ErrTopicDoesNotExist  = errors.New(errTopicDoesNotExist)
	ErrTopicAlreadyExists = errors.New(errTopicAlreadyExists)
	ErrInvalidHeaderSizes = errors.New(errInvalidHeaderSizes)
	ErrInvalidHeaderLimit = errors.New(errInvalidHeaderLimit)
	ErrInvalidMessageID   = errors.New(errInvalidMessageID)
	ErrInvalidTopic       = errors.New(errInvalidTopic)
	ErrInvalidBodyMissing = errors.New(errInvalidBodyMissing)
	ErrInvalidBodyJSON    = errors.New(errInvalidBodyJSON)
	ErrNoContent          = errors.New(errNoContent)
)

func SetError(w http.ResponseWriter, err error) {
	if err == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	err = errors.Cause(err)
	h := w.Header()
	h[HeaderErrors] = []string{err.Error()}
	switch err {
	case ErrTopicDoesNotExist, ErrTopicAlreadyExists:
		w.WriteHeader(http.StatusPreconditionFailed)
	case ErrInvalidHeaderSizes, ErrInvalidHeaderLimit, ErrInvalidMessageID, ErrInvalidTopic, ErrInvalidBodyMissing, ErrInvalidBodyJSON:
		w.WriteHeader(http.StatusBadRequest)
	case ErrNoContent:
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}
	return
}

func ReadErrors(header http.Header) error {
	errs := header[HeaderErrors]
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
		case errInvalidBodyMissing:
			return ErrInvalidBodyMissing
		case errInvalidBodyJSON:
			return ErrInvalidBodyJSON
		case errNoContent:
			return ErrNoContent
		default:
			return errors.New(err)
		}
	}
	return nil
}

func ReadSizes(header http.Header) ([]int64, error) {
	sizes := header[HeaderSizes]
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

type ModifyRequest struct {
	Truncate int64 `json:"truncate,omitempty"`
}

type TopicInfo struct {
	MinOffset int64 `json:"minOffset"`
	MaxOffset int64 `json:"maxOffset"`
}
