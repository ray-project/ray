/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package log

import (
	"github.com/go-logr/logr"
)

// NB: this is the same as the null logger logr/testing,
// but avoids accidentally adding the testing flags to
// all binaries.

// NullLogSink is a logr.Logger that does nothing.
type NullLogSink struct{}

var _ logr.LogSink = NullLogSink{}

// Init implements logr.LogSink.
func (log NullLogSink) Init(logr.RuntimeInfo) {
}

// Info implements logr.InfoLogger.
func (NullLogSink) Info(_ int, _ string, _ ...interface{}) {
	// Do nothing.
}

// Enabled implements logr.InfoLogger.
func (NullLogSink) Enabled(level int) bool {
	return false
}

// Error implements logr.Logger.
func (NullLogSink) Error(_ error, _ string, _ ...interface{}) {
	// Do nothing.
}

// WithName implements logr.Logger.
func (log NullLogSink) WithName(_ string) logr.LogSink {
	return log
}

// WithValues implements logr.Logger.
func (log NullLogSink) WithValues(_ ...interface{}) logr.LogSink {
	return log
}
