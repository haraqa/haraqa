package server

import (
	"io"
	"net/http"
	"regexp"

	"github.com/haraqa/haraqa/internal/headers"

	"github.com/haraqa/haraqa/internal/filequeue"
)

//go:generate go run github.com/golang/mock/mockgen -source queue.go -package server -destination queue_mock_test.go
//go:generate go run golang.org/x/tools/cmd/goimports -w queue_mock_test.go

var _ Queue = &filequeue.FileQueue{}

// Queue is the interface used by the server to produce and consume messages from different distinct categories called topics
type Queue interface {
	RootDir() string
	Close() error

	GetTopicOwner(topic string) (string, error)
	ListTopics(regex *regexp.Regexp) ([]string, error)
	CreateTopic(topic string) error
	DeleteTopic(topic string) error
	ModifyTopic(topic string, request headers.ModifyRequest) (*headers.TopicInfo, error)

	Produce(topic string, msgSizes []int64, timestamp uint64, r io.Reader) error
	Consume(group, topic string, id int64, limit int64, w http.ResponseWriter) (int, error)
}
