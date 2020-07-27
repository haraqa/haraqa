package queue

import (
	"io"
	"time"
)

type Queue interface {
	RootDir() string
	Close() error

	ListTopics() ([]string, error)
	CreateTopic(topic string) error
	DeleteTopic(topic string) error

	TruncateTopic(topic string, id int64) (*TopicInfo, error)
	InspectTopic(topic string) (*TopicInfo, error)

	Produce(topic string, msgSizes []int64, r io.Reader) error
	Consume(topic string, id int64, n int64) (*ConsumeInfo, error)
}

type TopicInfo struct {
	MinOffset int64
	MaxOffset int64
}

type ConsumeInfo struct {
	Filename  string
	File      io.ReadSeeker
	Exists    bool
	StartAt   uint64
	EndAt     uint64
	StartTime time.Time
	EndTime   time.Time
	Sizes     []int64
}
