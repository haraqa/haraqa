package server

// Logger is a handler for log messages of varying severities
type Logger interface {
	Errorf(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Debugf(format string, args ...interface{})
}

var _ Logger = noopLogger{}

type noopLogger struct{}

func (noopLogger) Errorf(format string, args ...interface{}) {}
func (noopLogger) Warnf(format string, args ...interface{})  {}
func (noopLogger) Infof(format string, args ...interface{})  {}
func (noopLogger) Debugf(format string, args ...interface{}) {}
