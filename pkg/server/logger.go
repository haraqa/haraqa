package server

import (
	"testing"
)

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

type TestLogger struct {
	TB testing.TB
}

func (t TestLogger) Errorf(format string, args ...interface{}) { t.TB.Logf("[ERROR]"+format, args...) }
func (t TestLogger) Warnf(format string, args ...interface{})  { t.TB.Logf("[WARN]"+format, args...) }
func (t TestLogger) Infof(format string, args ...interface{})  { t.TB.Logf("[INFO]"+format, args...) }
func (t TestLogger) Debugf(format string, args ...interface{}) { t.TB.Logf("[DEBUG]"+format, args...) }
