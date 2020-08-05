package server

import (
	"io"

	"github.com/haraqa/haraqa/protocol"

	"github.com/haraqa/haraqa/server/queue"
)

//go:generate mockgen -source queue.go -package server -destination queue_mock_test.go
//go:generate goimports -w queue_mock_test.go

var _ Queue = &queue.FileQueue{}

type Queue interface {
	RootDir() string
	Close() error

	ListTopics() ([]string, error)
	CreateTopic(topic string) error
	DeleteTopic(topic string) error

	TruncateTopic(topic string, id int64) (*protocol.TopicInfo, error)
	InspectTopic(topic string) (*protocol.TopicInfo, error)

	Produce(topic string, msgSizes []int64, r io.Reader) error
	Consume(topic string, id int64, n int64) (*protocol.ConsumeInfo, error)
}
