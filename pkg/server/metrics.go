package server

// Metrics allows for custom metric handlers for counting the number of messages and/or batch size
type Metrics interface {
	ProduceMsgs(int)
	ConsumeMsgs(int)
}

var _ Metrics = noOpMetrics{}

type noOpMetrics struct{}

func (noOpMetrics) ProduceMsgs(int) {}
func (noOpMetrics) ConsumeMsgs(int) {}
