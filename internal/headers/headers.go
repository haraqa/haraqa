package headers

import (
	"net/http"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// Headers using Canonical MIME structure
const (
	HeaderErrors        = "X-Errors"
	HeaderSizes         = "X-Sizes"
	HeaderStartTime     = "X-Start-Time"
	HeaderEndTime       = "X-End-Time"
	HeaderFileName      = "X-File-Name"
	HeaderWatchTopics   = "X-Topics"
	HeaderConsumerGroup = "X-Consumer-Group"
	ContentType         = "Content-Type"
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
	errInvalidWebsocket    = "invalid websocket"
	errNoContent           = "no content"
	errClosed              = "server closing"
	errProxyFailed         = "proxy failed"
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
	ErrInvalidWebsocket    = errors.New(errInvalidWebsocket)
	ErrNoContent           = errors.New(errNoContent)
	ErrClosed              = errors.New(errClosed)
	ErrProxyFailed         = errors.New(errProxyFailed)
)

var errMap = map[string]error{
	errTopicDoesNotExist:   ErrTopicDoesNotExist,
	errTopicAlreadyExists:  ErrTopicAlreadyExists,
	errInvalidHeaderSizes:  ErrInvalidHeaderSizes,
	errInvalidMessageID:    ErrInvalidMessageID,
	errInvalidMessageLimit: ErrInvalidMessageLimit,
	errInvalidTopic:        ErrInvalidTopic,
	errInvalidBodyMissing:  ErrInvalidBodyMissing,
	errInvalidBodyJSON:     ErrInvalidBodyJSON,
	errInvalidWebsocket:    ErrInvalidWebsocket,
	errNoContent:           ErrNoContent,
	errClosed:              ErrClosed,
	errProxyFailed:         ErrProxyFailed,
}

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
	case
		ErrInvalidHeaderSizes,
		ErrInvalidMessageID,
		ErrInvalidMessageLimit,
		ErrInvalidTopic,
		ErrInvalidBodyMissing,
		ErrInvalidBodyJSON,
		ErrInvalidWebsocket:
		w.WriteHeader(http.StatusBadRequest)
	case ErrNoContent:
		w.WriteHeader(http.StatusNoContent)
	case ErrClosed:
		w.WriteHeader(http.StatusServiceUnavailable)
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}
	_, _ = w.Write([]byte(errOriginal.Error()))
}

// ReadErrors reads any errors from the response header and returns as an error type
func ReadErrors(header http.Header) error {
	errs := header[HeaderErrors]
	if len(errs) == 0 {
		return nil
	}
	for _, err := range errs {
		if err == "" {
			continue
		}
		e, ok := errMap[err]
		if !ok {
			return errors.New(err)
		}
		return e
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
