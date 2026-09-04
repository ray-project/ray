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

import "github.com/go-logr/logr"

// NullLogSink is a no-op LogSink implementation that discards all logs.
// Used for initialization and as the default empty log implementation.
type NullLogSink struct{}

// Init does nothing.
func (n *NullLogSink) Init(info logr.RuntimeInfo) {}

// Enabled always returns false, disabling all log levels.
func (n *NullLogSink) Enabled(level int) bool { return false }

// Info outputs nothing.
func (n *NullLogSink) Info(level int, msg string, keysAndValues ...interface{}) {}

// Error outputs nothing.
func (n *NullLogSink) Error(err error, msg string, keysAndValues ...interface{}) {}

// WithName returns itself, behavior unchanged.
func (n *NullLogSink) WithName(name string) logr.LogSink { return n }

// WithValues returns itself, behavior unchanged.
func (n *NullLogSink) WithValues(keysAndValues ...interface{}) logr.LogSink { return n }