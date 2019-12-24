package protocol

import (
	"github.com/pkg/errors"
)

//go:generate protoc --gogofaster_out=plugins=grpc:. grpc.proto

var (
	TopicExistsErr    = errors.New("topic already exists")
	ErrorUndefined    = errors.New("undefined error occurred")
	TopicDoesNotExist = errors.New("topic does not exist")
)

func ErrorToResponse(err error) [2]byte {
	if err == nil {
		return [2]byte{0, 0}
	}
	switch errors.Cause(err) {
	case TopicDoesNotExist:
		return [2]byte{0, 1}
	default:
		return [2]byte{255, 255}
	}
}

func ResponseToError(resp [2]byte) error {
	switch resp {
	case [2]byte{0, 0}:
		return nil
	case [2]byte{0, 1}:
		return TopicDoesNotExist
	case [2]byte{255, 255}:
		return ErrorUndefined
	default:
		return ErrorUndefined
	}
	return nil
}
