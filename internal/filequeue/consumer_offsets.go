package filequeue

// SetConsumerOffset sets the offset for a given consumer group + topic
func (q *FileQueue) SetConsumerOffset(group, topic string, id int64) error {
	return nil
}
