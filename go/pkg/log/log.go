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

// Package log provides structured logging interface and global Logger.
// Design inspired by controller-runtime logging.
package log

import (
	"github.com/go-logr/logr"
)

// Log is the global Logger, initialized with DelegatingLogSink wrapping NullLogSink.
// After calling SetLogger, all logs will be routed to the actual implementation.
var Log logr.Logger

// SetLogger sets the underlying implementation of the global Logger.
// Passing nil/Discard Logger will reset to NullLogSink.
func SetLogger(l logr.Logger) {
	if l.GetSink() == nil {
		Log = logr.New(&NullLogSink{})
		return
	}
	if delegate, ok := Log.GetSink().(*DelegatingLogSink); ok {
		delegate.Fulfill(l.GetSink())
	} else {
		Log = l
	}
}

func init() {
	Log = logr.New(NewDelegatingLogSink())
}

// WithName returns a Logger with the specified name.
// This is a convenience method for Log.WithName(name).
func WithName(name string) logr.Logger {
	return Log.WithName(name)
}

// WithValues returns a Logger with preset key-value pairs.
// This is a convenience method for Log.WithValues(keysAndValues...).
func WithValues(keysAndValues ...interface{}) logr.Logger {
	return Log.WithValues(keysAndValues...)
}
