// Copyright 2026 The Ray Authors.
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

package event

import (
	"os"

	"github.com/go-logr/logr"
)

// fileLogSink is a simple logr.LogSink that writes directly to a file.
type fileLogSink struct {
	file *os.File
}

// NewFileLogger creates a file-backed logr logger.
func NewFileLogger(file *os.File) logr.Logger {
	sink := &fileLogSink{file: file}
	return logr.New(sink)
}

// Init implements logr.LogSink.
func (f *fileLogSink) Init(info logr.RuntimeInfo) {
}

// Enabled implements logr.LogSink.
func (f *fileLogSink) Enabled(level int) bool {
	return true
}

// Info implements logr.LogSink.
func (f *fileLogSink) Info(level int, msg string, keysAndValues ...interface{}) {
	// Write the message as-is, without extra formatting.
	f.file.Write([]byte(msg))
	f.file.Write([]byte("\n"))
	f.file.Sync()
}

// Error implements logr.LogSink.
func (f *fileLogSink) Error(err error, msg string, keysAndValues ...interface{}) {
	// Write the message as-is, without extra formatting.
	f.file.Write([]byte(msg))
	f.file.Write([]byte("\n"))
	f.file.Sync()
}

// WithValues implements logr.LogSink.
func (f *fileLogSink) WithValues(keysAndValues ...interface{}) logr.LogSink {
	// Trivial implementation: return the receiver and ignore the extra
	// key-value pairs, because the event JSON already carries all the
	// information we need.
	return f
}

// WithName implements logr.LogSink.
func (f *fileLogSink) WithName(name string) logr.LogSink {
	// Trivial implementation: return the receiver and ignore the name,
	// because the event JSON already carries all the information we need.
	return f
}
