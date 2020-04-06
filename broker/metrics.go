package broker

// Metrics allows for custom metrics tracking
type Metrics interface {
	AddProduceMsgs([]byte, []int64)
	AddConsumeMsgs([]byte, []int64)
}

type noopMetrics struct{}

func (noopMetrics) AddProduceMsgs([]byte, []int64) {}
func (noopMetrics) AddConsumeMsgs([]byte, []int64) {}
