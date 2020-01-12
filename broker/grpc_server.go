package broker

import (
	"context"
	"log"
	"os"

	"github.com/haraqa/haraqa/internal/protocol"
)

// CreateTopic implements protocol.HaraqaServer CreateTopic
func (b *Broker) CreateTopic(ctx context.Context, in *protocol.CreateTopicRequest) (*protocol.CreateTopicResponse, error) {
	err := b.Q.CreateTopic(in.GetTopic())
	if err != nil {
		return &protocol.CreateTopicResponse{Meta: &protocol.Meta{OK: false, ErrorMsg: err.Error()}}, nil
	}

	return &protocol.CreateTopicResponse{Meta: &protocol.Meta{OK: true}}, nil
}

// DeleteTopic implements protocol.HaraqaServer CreateTopic
func (b *Broker) DeleteTopic(ctx context.Context, in *protocol.DeleteTopicRequest) (*protocol.DeleteTopicResponse, error) {
	err := b.Q.DeleteTopic(in.GetTopic())
	if err != nil {
		return &protocol.DeleteTopicResponse{Meta: &protocol.Meta{OK: false, ErrorMsg: err.Error()}}, nil
	}

	return &protocol.DeleteTopicResponse{Meta: &protocol.Meta{OK: true}}, nil
}

// ListTopics implements protocol.HaraqaServer ListTopics
func (b *Broker) ListTopics(ctx context.Context, in *protocol.ListTopicsRequest) (*protocol.ListTopicsResponse, error) {
	topics, err := b.Q.ListTopics(in.GetPrefix(), in.GetSuffix(), in.GetRegex())
	if err != nil {
		return &protocol.ListTopicsResponse{Meta: &protocol.Meta{OK: false, ErrorMsg: err.Error()}}, nil
	}

	return &protocol.ListTopicsResponse{Meta: &protocol.Meta{OK: true}, Topics: topics}, nil
}

// TruncateTopic implements protocol.HaraqaServer TruncateTopic
func (b *Broker) TruncateTopic(ctx context.Context, in *protocol.TruncateTopicRequest) (*protocol.TruncateTopicResponse, error) {
	log.Printf("Received: %v", in.GetTopic())
	return &protocol.TruncateTopicResponse{Meta: &protocol.Meta{OK: true}}, nil
}

// Offsets implements protocol.HaraqaServer Offset
func (b *Broker) Offsets(ctx context.Context, in *protocol.OffsetRequest) (*protocol.OffsetResponse, error) {
	min, max, err := b.Q.Offsets(in.GetTopic())
	if err != nil {
		if err == os.ErrNotExist {
			err = protocol.ErrTopicDoesNotExist
		}
		return &protocol.OffsetResponse{Meta: &protocol.Meta{OK: false, ErrorMsg: err.Error()}}, nil
	}

	return &protocol.OffsetResponse{Meta: &protocol.Meta{OK: true}, MinOffset: min, MaxOffset: max}, nil
}
