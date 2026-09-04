package log

import (
	"github.com/go-logr/logr"
)

// testLogSink is a LogSink implementation for testing that records the last
// received message and key-value pairs.
type testLogSink struct {
	lastMsg           string
	lastKeysAndValues []interface{}
	lastErr           error
	lastLevel         int
	lastName          string
}

func (t *testLogSink) Init(info logr.RuntimeInfo) {}

func (t *testLogSink) Enabled(level int) bool { return true }

func (t *testLogSink) Info(level int, msg string, keysAndValues ...interface{}) {
	t.lastLevel = level
	t.lastMsg = msg
	t.lastKeysAndValues = keysAndValues
}

func (t *testLogSink) Error(err error, msg string, keysAndValues ...interface{}) {
	t.lastErr = err
	t.lastMsg = msg
	t.lastKeysAndValues = keysAndValues
}

func (t *testLogSink) V(level int) logr.LogSink { return t }

func (t *testLogSink) WithName(name string) logr.LogSink {
	t.lastName = name
	return &testLogSinkWithName{
		testLogSink: t,
		name:        name,
	}
}

func (t *testLogSink) WithValues(keysAndValues ...interface{}) logr.LogSink {
	return &testLogSinkWithValues{
		testLogSink:   t,
		keysAndValues: keysAndValues,
	}
}

type testLogSinkWithName struct {
	*testLogSink
	name string
}

func (t *testLogSinkWithName) Info(level int, msg string, keysAndValues ...interface{}) {
	t.testLogSink.lastName = t.name
	t.testLogSink.Info(level, t.name+": "+msg, keysAndValues...)
}

func (t *testLogSinkWithName) Error(err error, msg string, keysAndValues ...interface{}) {
	t.testLogSink.lastName = t.name
	t.testLogSink.Error(err, t.name+": "+msg, keysAndValues...)
}

func (t *testLogSinkWithName) WithName(name string) logr.LogSink {
	return &testLogSinkWithName{
		testLogSink: t.testLogSink,
		name:        t.name + "." + name,
	}
}

func (t *testLogSinkWithName) WithValues(keysAndValues ...interface{}) logr.LogSink {
	return &testLogSinkWithValues{
		testLogSink:   t.testLogSink,
		keysAndValues: keysAndValues,
		name:          t.name,
	}
}

type testLogSinkWithValues struct {
	*testLogSink
	keysAndValues []interface{}
	name          string
}

func (t *testLogSinkWithValues) Info(level int, msg string, keysAndValues ...interface{}) {
	prefix := ""
	if t.name != "" {
		prefix = t.name + ": "
		t.testLogSink.lastName = t.name
	}
	t.testLogSink.Info(level, prefix+msg, append(t.keysAndValues, keysAndValues...)...)
}

func (t *testLogSinkWithValues) Error(err error, msg string, keysAndValues ...interface{}) {
	prefix := ""
	if t.name != "" {
		prefix = t.name + ": "
		t.testLogSink.lastName = t.name
	}
	t.testLogSink.Error(err, prefix+msg, append(t.keysAndValues, keysAndValues...)...)
}

func (t *testLogSinkWithValues) WithName(name string) logr.LogSink {
	newName := name
	if t.name != "" {
		newName = t.name + "." + name
	}
	return &testLogSinkWithValues{
		testLogSink:   t.testLogSink,
		keysAndValues: t.keysAndValues,
		name:          newName,
	}
}

func (t *testLogSinkWithValues) WithValues(keysAndValues ...interface{}) logr.LogSink {
	return &testLogSinkWithValues{
		testLogSink:   t.testLogSink,
		keysAndValues: append(t.keysAndValues, keysAndValues...),
		name:          t.name,
	}
}