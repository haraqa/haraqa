package memconsumer

import "sync"

type Manager struct {
	m sync.Map
}

type muxID struct {
	sync.Mutex
	id int64
}

func New() *Manager {
	return &Manager{}
}

func (m *Manager) GetOffset(group, topic string, reqID int64) (int64, func(), error) {
	if group == "" {
		return reqID, func() {}, nil
	}
	actual, _ := m.m.LoadOrStore(group+"/"+topic, &muxID{id: 0})
	mID := actual.(*muxID)
	mID.Lock()
	if reqID >= 0 {
		return reqID, mID.Unlock, nil
	}
	return mID.id, mID.Unlock, nil
}

func (m *Manager) SetOffset(group, topic string, id int64) error {
	if group == "" {
		return nil
	}
	actual, loaded := m.m.LoadOrStore(group+"/"+topic, &muxID{id: id})
	if !loaded {
		return nil
	}
	mID := actual.(*muxID)
	mID.Lock()
	defer mID.Unlock()
	if mID.id < id {
		mID.id = id
	}
	return nil
}
