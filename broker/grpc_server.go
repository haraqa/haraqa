package broker

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/haraqa/haraqa/internal/protocol"
	"github.com/pkg/errors"
	"gopkg.in/fsnotify.v1"
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

// WatchTopics implements protocol.HaraqaServer WatchTopics
func (b *Broker) WatchTopics(srv protocol.Haraqa_WatchTopicsServer) error {
	req, err := srv.Recv()
	if err != nil {
		// best effort send error response
		_ = srv.Send(&protocol.WatchResponse{Meta: &protocol.Meta{OK: false, ErrorMsg: err.Error()}})
		return err
	}

	offsets := make(map[string][2]int64)
	topics := req.GetTopics()
	if len(topics) == 0 {
		return nil
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		// best effort send error response
		_ = srv.Send(&protocol.WatchResponse{Meta: &protocol.Meta{OK: false, ErrorMsg: err.Error()}})
		return err
	}
	defer watcher.Close()

	for _, topic := range topics {
		filename := filepath.Join(b.config.Volumes[len(b.config.Volumes)-1], string(topic))
		err = watcher.Add(filename)
		if err != nil {
			err = errors.Wrapf(err, "unable to watch topic %v", string(topic))
			// best effort send error response
			_ = srv.Send(&protocol.WatchResponse{Meta: &protocol.Meta{OK: false, ErrorMsg: err.Error()}})
			return err
		}

		// get file offsets
		min, max, err := b.Q.Offsets(topic)
		if os.IsNotExist(errors.Cause(err)) {
			offsets[string(topic)] = [2]int64{0, -1}
			// no need to send initial offsets, none exist yet
			continue
		}
		if err != nil {
			err = errors.Wrapf(err, "unable to get topic offsets for %v", string(topic))
			// best effort send error response
			_ = srv.Send(&protocol.WatchResponse{Meta: &protocol.Meta{OK: false, ErrorMsg: err.Error()}})
			return err
		}
		offsets[string(topic)] = [2]int64{min, max}
	}

	// send an ack
	err = srv.Send(&protocol.WatchResponse{Meta: &protocol.Meta{OK: true}})
	if err != nil {
		return err
	}

	for topic, offsets := range offsets {
		if offsets[1] < 0 {
			continue
		}
		// send current offsets
		err = srv.Send(&protocol.WatchResponse{
			Meta:      &protocol.Meta{OK: true},
			Topic:     []byte(topic),
			MinOffset: offsets[0],
			MaxOffset: offsets[1],
		})
		if err != nil {
			return err
		}
	}

	errs := make(chan error, 2)

	// send new file offsets to the connection
	go b.watch(srv, watcher, errs, offsets)

	// read from the connetion, wait for a term signal
	go b.watchTerm(srv, errs)

	err = <-errs
	if err != nil {
		// best effort send error response
		_ = srv.Send(&protocol.WatchResponse{Meta: &protocol.Meta{OK: false, ErrorMsg: err.Error()}})
		return err
	}
	return nil
}

func (b *Broker) watchTerm(srv protocol.Haraqa_WatchTopicsServer, errs chan error) {
	for {
		req, err := srv.Recv()
		if err != nil {
			errs <- err
			return
		}
		if req.GetTerm() {
			errs <- nil
			return
		}
	}
}

func (b *Broker) watch(srv protocol.Haraqa_WatchTopicsServer, watcher *fsnotify.Watcher, errs chan error, offsets map[string][2]int64) {
loop:
	for {
		select {
		// watch for events
		case event := <-watcher.Events:
			if event.Op != fsnotify.Write || !strings.HasSuffix(event.Name, ".dat") {
				continue loop
			}

			topic := filepath.Base(filepath.Dir(event.Name))

			dat, err := os.Open(event.Name)
			if err != nil {
				errs <- errors.Wrapf(err, "trouble getting topic data for %s", topic)
				return
			}

			info, err := dat.Stat()
			if err != nil {
				errs <- errors.Wrapf(err, "trouble getting topic data for %s", topic)
				return
			}
			o, ok := offsets[topic]
			if !ok {
				continue
			}
			o[1] = info.Size() / 24
			offsets[topic] = o

			// send current offsets
			err = srv.Send(&protocol.WatchResponse{
				Meta:      &protocol.Meta{OK: true},
				Topic:     []byte(topic),
				MinOffset: o[0],
				MaxOffset: o[1],
			})
			if err != nil {
				errs <- err
				return
			}

			// watch for errors
		case err := <-watcher.Errors:
			errs <- errors.Wrap(err, "watcher failed")
			return
		}
	}
}

// Lock implements protocol.HaraqaServer Lock
func (b *Broker) Lock(srv protocol.Haraqa_LockServer) error {
	var locked bool
	var group []byte
	t := time.NewTimer(time.Second * 30)

	defer func() {
		if locked {
			b.releaseGroupLock(group)
		}
	}()

	for {
		req, err := srv.Recv()
		if err != nil {
			return err
		}

		if req.Group != nil {
			group = req.Group
		}

		if req.GetLock() {
			t.Reset(time.Duration(req.GetTime()) * time.Millisecond)
			locked = b.getGroupLock(group, t)
		} else {
			b.releaseGroupLock(group)
			locked = false
		}

		err = srv.Send(&protocol.LockResponse{
			Meta:   &protocol.Meta{OK: true},
			Locked: locked,
		})
		if err != nil {
			return err
		}
	}
}
