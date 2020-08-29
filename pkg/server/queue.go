package server

import (
	"io"
	"net/http"

	"github.com/haraqa/haraqa/internal/headers"

	"github.com/haraqa/haraqa/internal/filequeue"
)

//go:generate mockgen -source queue.go -package server -destination queue_mock_test.go
//go:generate goimports -w queue_mock_test.go

var _ Queue = &filequeue.FileQueue{}

type Queue interface {
	RootDir() string
	Close() error

	ListTopics() ([]string, error)
	CreateTopic(topic string) error
	DeleteTopic(topic string) error
	ModifyTopic(topic string, request headers.ModifyRequest) (*headers.TopicInfo, error)

	Produce(topic string, msgSizes []int64, timestamp uint64, r io.Reader) error
	Consume(topic string, id int64, limit int64, w http.ResponseWriter) (int, error)
}
