package server

type Metrics interface {
	ProduceMsgs(int)
	ConsumeMsgs(int)
}

var _ Metrics = noOpMetrics{}

type noOpMetrics struct{}

func (noOpMetrics) ProduceMsgs(int) {}
func (noOpMetrics) ConsumeMsgs(int) {}
