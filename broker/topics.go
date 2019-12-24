package broker

/*
type topics struct {
	sync.Mutex
	prod map[string]*multifile.File
}

func (b *Broker) getProducerTopicFile(topic []byte) (*multifile.File, error) {
	var err error
	b.topics.Lock()
	defer b.topics.Unlock()
	f, ok := b.topics.prod[string(topic)]
	if !ok {
		f, err = multifile.Create(b.config.Volumes, string(topic))
		if err != nil {
			return nil, err
		}
		b.topics.prod[string(topic)] = f
	}
	return f, nil
}

func (b *Broker) getConsumerTopicFile(topic []byte) (*multifile.File, error) {
	var err error
	b.topics.Lock()
	defer b.topics.Unlock()
	f, ok := b.topics.prod[string(topic)]
	if !ok {
		f, err = multifile.Open(b.config.Volumes, string(topic))
		if err != nil {
			return nil, err
		}
		b.topics.prod[string(topic)] = f
	}
	return f, nil
}
*/
