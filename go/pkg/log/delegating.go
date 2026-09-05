// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"sync"

	"github.com/go-logr/logr"
)

// DelegatingLogSink is a logr.LogSink that delegates all operations until the actual logger is configured.
// This allows obtaining a logger in init() or other early code, while actual configuration happens in main().
//
// Design inspired by controller-runtime's delegatingLogSink implementation.
type DelegatingLogSink struct {
	mu        sync.RWMutex
	sink      logr.LogSink
	fulfilled bool
}

// NewDelegatingLogSink creates a new DelegatingLogSink.
func NewDelegatingLogSink() *DelegatingLogSink {
	return &DelegatingLogSink{
		sink: &NullLogSink{}, // NullLogSink implements the full LogSink interface
	}
}

// Fulfill sets the actual LogSink implementation.
func (d *DelegatingLogSink) Fulfill(sink logr.LogSink) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.sink = sink
	d.fulfilled = true
}

// Init initializes the log sink.
func (d *DelegatingLogSink) Init(info logr.RuntimeInfo) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	d.sink.Init(info)
}

// Enabled checks if the log level is enabled.
func (d *DelegatingLogSink) Enabled(level int) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.sink.Enabled(level)
}

// Info logs an info-level message.
func (d *DelegatingLogSink) Info(level int, msg string, keysAndValues ...interface{}) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	d.sink.Info(level, msg, keysAndValues...)
}

// Error logs an error-level message.
func (d *DelegatingLogSink) Error(err error, msg string, keysAndValues ...interface{}) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	d.sink.Error(err, msg, keysAndValues...)
}

// WithName returns a LogSink with the name added.
func (d *DelegatingLogSink) WithName(name string) logr.LogSink {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return &delegatingLogSinkWithName{
		delegate: d,
		name:     name,
	}
}

// WithValues returns a LogSink with key-value pairs added.
func (d *DelegatingLogSink) WithValues(keysAndValues ...interface{}) logr.LogSink {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return &delegatingLogSinkWithValues{
		delegate:      d,
		keysAndValues: keysAndValues,
	}
}

// delegatingLogSinkWithName is a delegating LogSink with a name.
type delegatingLogSinkWithName struct {
	delegate *DelegatingLogSink
	name     string
}

func (d *delegatingLogSinkWithName) Init(info logr.RuntimeInfo) {
	d.delegate.mu.RLock()
	defer d.delegate.mu.RUnlock()
	if d.delegate.fulfilled {
		d.delegate.sink.WithName(d.name).Init(info)
	} else {
		d.delegate.sink.Init(info)
	}
}

func (d *delegatingLogSinkWithName) Enabled(level int) bool {
	d.delegate.mu.RLock()
	defer d.delegate.mu.RUnlock()
	if d.delegate.fulfilled {
		return d.delegate.sink.WithName(d.name).Enabled(level)
	}
	return d.delegate.sink.Enabled(level)
}

func (d *delegatingLogSinkWithName) Info(level int, msg string, keysAndValues ...interface{}) {
	d.delegate.mu.RLock()
	defer d.delegate.mu.RUnlock()
	if d.delegate.fulfilled {
		d.delegate.sink.WithName(d.name).Info(level, msg, keysAndValues...)
	} else {
		d.delegate.sink.Info(level, msg, keysAndValues...)
	}
}

func (d *delegatingLogSinkWithName) Error(err error, msg string, keysAndValues ...interface{}) {
	d.delegate.mu.RLock()
	defer d.delegate.mu.RUnlock()
	if d.delegate.fulfilled {
		d.delegate.sink.WithName(d.name).Error(err, msg, keysAndValues...)
	} else {
		d.delegate.sink.Error(err, msg, keysAndValues...)
	}
}

func (d *delegatingLogSinkWithName) WithName(name string) logr.LogSink {
	d.delegate.mu.RLock()
	defer d.delegate.mu.RUnlock()
	// Accumulate names
	newName := d.name + "." + name
	if d.delegate.fulfilled {
		return d.delegate.sink.WithName(newName)
	}
	return &delegatingLogSinkWithName{
		delegate: d.delegate,
		name:     newName,
	}
}

func (d *delegatingLogSinkWithName) WithValues(keysAndValues ...interface{}) logr.LogSink {
	d.delegate.mu.RLock()
	defer d.delegate.mu.RUnlock()
	if d.delegate.fulfilled {
		return d.delegate.sink.WithName(d.name).WithValues(keysAndValues...)
	}
	return &delegatingLogSinkWithValues{
		delegate:      d.delegate,
		keysAndValues: keysAndValues,
		name:          d.name,
	}
}

// delegatingLogSinkWithValues is a delegating LogSink with key-value pairs.
type delegatingLogSinkWithValues struct {
	delegate      *DelegatingLogSink
	keysAndValues []interface{}
	name          string // optional name
}

func (d *delegatingLogSinkWithValues) Init(info logr.RuntimeInfo) {
	d.delegate.mu.RLock()
	defer d.delegate.mu.RUnlock()
	if d.delegate.fulfilled {
		sink := d.delegate.sink
		if d.name != "" {
			sink = sink.WithName(d.name)
		}
		sink.WithValues(d.keysAndValues...).Init(info)
	} else {
		d.delegate.sink.Init(info)
	}
}

func (d *delegatingLogSinkWithValues) Enabled(level int) bool {
	d.delegate.mu.RLock()
	defer d.delegate.mu.RUnlock()
	if d.delegate.fulfilled {
		sink := d.delegate.sink
		if d.name != "" {
			sink = sink.WithName(d.name)
		}
		return sink.WithValues(d.keysAndValues...).Enabled(level)
	}
	return d.delegate.sink.Enabled(level)
}

func (d *delegatingLogSinkWithValues) Info(level int, msg string, keysAndValues ...interface{}) {
	d.delegate.mu.RLock()
	defer d.delegate.mu.RUnlock()
	if d.delegate.fulfilled {
		sink := d.delegate.sink
		if d.name != "" {
			sink = sink.WithName(d.name)
		}
		sink.WithValues(d.keysAndValues...).Info(level, msg, keysAndValues...)
	} else {
		d.delegate.sink.Info(level, msg, append(d.keysAndValues, keysAndValues...)...)
	}
}

func (d *delegatingLogSinkWithValues) Error(err error, msg string, keysAndValues ...interface{}) {
	d.delegate.mu.RLock()
	defer d.delegate.mu.RUnlock()
	if d.delegate.fulfilled {
		sink := d.delegate.sink
		if d.name != "" {
			sink = sink.WithName(d.name)
		}
		sink.WithValues(d.keysAndValues...).Error(err, msg, keysAndValues...)
	} else {
		d.delegate.sink.Error(err, msg, append(d.keysAndValues, keysAndValues...)...)
	}
}

func (d *delegatingLogSinkWithValues) WithName(name string) logr.LogSink {
	d.delegate.mu.RLock()
	defer d.delegate.mu.RUnlock()
	if d.delegate.fulfilled {
		sink := d.delegate.sink
		if d.name != "" {
			sink = sink.WithName(d.name + "." + name)
		} else {
			sink = sink.WithName(name)
		}
		return sink.WithValues(d.keysAndValues...)
	}
	return &delegatingLogSinkWithValues{
		delegate:      d.delegate,
		keysAndValues: d.keysAndValues,
		name:          d.name + "." + name,
	}
}

func (d *delegatingLogSinkWithValues) WithValues(keysAndValues ...interface{}) logr.LogSink {
	d.delegate.mu.RLock()
	defer d.delegate.mu.RUnlock()
	if d.delegate.fulfilled {
		sink := d.delegate.sink
		if d.name != "" {
			sink = sink.WithName(d.name)
		}
		return sink.WithValues(append(d.keysAndValues, keysAndValues...)...)
	}
	return &delegatingLogSinkWithValues{
		delegate:      d.delegate,
		keysAndValues: append(d.keysAndValues, keysAndValues...),
		name:          d.name,
	}
}
