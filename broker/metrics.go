package broker

// Metrics allows for custom metrics tracking
type Metrics interface {
	AddProduceMsgs(int)
	AddConsumeMsgs(int)
}

type noopMetrics struct{}

func (noopMetrics) AddProduceMsgs(int) {}
func (noopMetrics) AddConsumeMsgs(int) {}
