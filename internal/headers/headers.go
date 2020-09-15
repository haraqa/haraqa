package headers

import (
	"net/http"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// Headers using Canonical MIME structure
const (
	HeaderErrors    = "X-Errors"
	HeaderSizes     = "X-Sizes"
	HeaderStartTime = "X-Start-Time"
	HeaderEndTime   = "X-End-Time"
	HeaderFileName  = "X-File-Name"
	ContentType     = "Content-Type"
)

const (
	errTopicDoesNotExist   = "topic does not exist"
	errTopicAlreadyExists  = "topic already exists"
	errInvalidHeaderSizes  = "invalid header: " + HeaderSizes
	errInvalidMessageID    = "invalid message id"
	errInvalidMessageLimit = "invalid message limit"
	errInvalidTopic        = "invalid topic"
	errInvalidBodyMissing  = "invalid body: body cannot be empty"
	errInvalidBodyJSON     = "invalid body: invalid json entry"
	errNoContent           = "no content"
)

// Errors returned by the Client/Server
var (
	ErrTopicDoesNotExist   = errors.New(errTopicDoesNotExist)
	ErrTopicAlreadyExists  = errors.New(errTopicAlreadyExists)
	ErrInvalidHeaderSizes  = errors.New(errInvalidHeaderSizes)
	ErrInvalidMessageID    = errors.New(errInvalidMessageID)
	ErrInvalidMessageLimit = errors.New(errInvalidMessageLimit)
	ErrInvalidTopic        = errors.New(errInvalidTopic)
	ErrInvalidBodyMissing  = errors.New(errInvalidBodyMissing)
	ErrInvalidBodyJSON     = errors.New(errInvalidBodyJSON)
	ErrNoContent           = errors.New(errNoContent)
)

// SetError adds the error to the response header and body and sets the status code as needed
func SetError(w http.ResponseWriter, errOriginal error) {
	if errOriginal == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	err := errors.Cause(errOriginal)
	h := w.Header()
	h[HeaderErrors] = []string{err.Error()}
	switch err {
	case ErrTopicDoesNotExist, ErrTopicAlreadyExists:
		w.WriteHeader(http.StatusPreconditionFailed)
	case ErrInvalidHeaderSizes, ErrInvalidMessageID, ErrInvalidMessageLimit, ErrInvalidTopic, ErrInvalidBodyMissing, ErrInvalidBodyJSON:
		w.WriteHeader(http.StatusBadRequest)
	case ErrNoContent:
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}
	_, _ = w.Write([]byte(errOriginal.Error()))
	return
}

// ReadErrors reads any errors from the response header and returns as an error type
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
		case errInvalidMessageID:
			return ErrInvalidMessageID
		case errInvalidMessageLimit:
			return ErrInvalidMessageLimit
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

// ReadSizes reads the message sizes from the header
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

// SetSizes sets the sizes of the messages in the header
func SetSizes(msgSizes []int64, h http.Header) http.Header {
	sizes := make([]string, len(msgSizes))
	for i := range msgSizes {
		sizes[i] = strconv.FormatInt(msgSizes[i], 10)
	}
	h[HeaderSizes] = sizes
	return h
}

// ModifyRequest is the request structure required by the modify endpoints
type ModifyRequest struct {
	Truncate int64     `json:"truncate,omitempty"`
	Before   time.Time `json:"before,omitempty"`
}

// TopicInfo is the response structure returned by the modify endpoints
type TopicInfo struct {
	MinOffset int64 `json:"minOffset"`
	MaxOffset int64 `json:"maxOffset"`
}
