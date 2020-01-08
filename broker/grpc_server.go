package broker

import (
	"context"
	"log"
	"os"

	"github.com/haraqa/haraqa/protocol"
	pb "github.com/haraqa/haraqa/protocol"
)

// CreateTopic implements protocol.HaraqaServer CreateTopic
func (b *Broker) CreateTopic(ctx context.Context, in *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	err := b.config.Queue.CreateTopic(in.GetTopic())
	if err != nil {
		return &pb.CreateTopicResponse{Meta: &pb.Meta{OK: false, ErrorMsg: err.Error()}}, nil
	}

	return &pb.CreateTopicResponse{Meta: &pb.Meta{OK: true}}, nil
}

// DeleteTopic implements protocol.HaraqaServer CreateTopic
func (b *Broker) DeleteTopic(ctx context.Context, in *pb.DeleteTopicRequest) (*pb.DeleteTopicResponse, error) {
	err := b.config.Queue.DeleteTopic(in.GetTopic())
	if err != nil {
		return &pb.DeleteTopicResponse{Meta: &pb.Meta{OK: false, ErrorMsg: err.Error()}}, nil
	}

	return &pb.DeleteTopicResponse{Meta: &pb.Meta{OK: true}}, nil
}

// ListTopics implements protocol.HaraqaServer ListTopics
func (b *Broker) ListTopics(ctx context.Context, in *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	topics, err := b.config.Queue.ListTopics()
	if err != nil {
		return &pb.ListTopicsResponse{Meta: &pb.Meta{OK: false, ErrorMsg: err.Error()}}, nil
	}

	return &pb.ListTopicsResponse{Meta: &pb.Meta{OK: true}, Topics: topics}, nil
}

func sum(s []int64) int64 {
	var out int64
	for _, v := range s {
		out += v
	}
	return out
}

// TruncateTopic implements protocol.HaraqaServer TruncateTopic
func (b *Broker) TruncateTopic(ctx context.Context, in *pb.TruncateTopicRequest) (*pb.TruncateTopicResponse, error) {
	log.Printf("Received: %v", in.GetTopic())
	return &pb.TruncateTopicResponse{Meta: &pb.Meta{OK: true}}, nil
}

// Offsets implements protocol.HaraqaServer Offset
func (b *Broker) Offsets(ctx context.Context, in *pb.OffsetRequest) (*pb.OffsetResponse, error) {
	min, max, err := b.config.Queue.Offsets(in.GetTopic())
	if err != nil {
		if err == os.ErrNotExist {
			err = protocol.ErrTopicDoesNotExist
		}
		return &pb.OffsetResponse{Meta: &pb.Meta{OK: false, ErrorMsg: err.Error()}}, nil
	}

	return &pb.OffsetResponse{Meta: &pb.Meta{OK: true}, MinOffset: min, MaxOffset: max}, nil
}
