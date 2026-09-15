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
	"testing"

	"github.com/go-logr/logr"
)

func TestSetLogger_NilLogger(t *testing.T) {
	// Reset Log to its initial state.
	Log = logr.New(NewDelegatingLogSink())

	SetLogger(logr.Logger{})
	Log.Info("test message", "key", "value")
	Log.Error(nil, "test error", "key", "value")
}

func TestSetLogger_ValidLogger(t *testing.T) {
	// Reset Log to its initial state.
	Log = logr.New(NewDelegatingLogSink())

	testSink := &testLogSink{}
	testLogger := logr.New(testSink)
	SetLogger(testLogger)
	Log.Info("test message", "key", "value")

	if testSink.lastMsg != "test message" {
		t.Errorf("expected 'test message', got '%s'", testSink.lastMsg)
	}
	if len(testSink.lastKeysAndValues) != 2 {
		t.Errorf("expected 2 keysAndValues, got %d", len(testSink.lastKeysAndValues))
	}
	if testSink.lastKeysAndValues[0] != "key" || testSink.lastKeysAndValues[1] != "value" {
		t.Errorf("expected ['key', 'value'], got %v", testSink.lastKeysAndValues)
	}
}

func TestSetLogger_ReplaceLogger(t *testing.T) {
	// Reset Log to its initial state.
	Log = logr.New(NewDelegatingLogSink())

	// First set.
	testSink1 := &testLogSink{}
	testLogger1 := logr.New(testSink1)
	SetLogger(testLogger1)

	// Second set directly replaces the logger, no longer using DelegatingLogSink.
	testSink2 := &testLogSink{}
	testLogger2 := logr.New(testSink2)
	Log = testLogger2

	Log.Info("second logger message")
	if testSink2.lastMsg != "second logger message" {
		t.Errorf("expected 'second logger message', got '%s'", testSink2.lastMsg)
	}
}

// TestSetLogger_NonDelegatingLogger tests replacing a non-DelegatingLogSink.
func TestSetLogger_NonDelegatingLogger(t *testing.T) {
	// Reset Log to its initial state.
	Log = logr.New(NewDelegatingLogSink())

	// Set a normal logger first.
	testSink1 := &testLogSink{}
	testLogger1 := logr.New(testSink1)
	SetLogger(testLogger1)
	Log.Info("first message", "key", "value1")

	// Replace with another logger.
	testSink2 := &testLogSink{}
	testLogger2 := logr.New(testSink2)
	SetLogger(testLogger2)
	Log.Info("second message", "key", "value2")

	if testSink2.lastMsg != "second message" {
		t.Errorf("expected 'second message', got '%s'", testSink2.lastMsg)
	}
}

func TestWithName(t *testing.T) {
	// Reset Log to its initial state.
	Log = logr.New(NewDelegatingLogSink())

	testSink := &testLogSink{}
	testLogger := logr.New(testSink)
	SetLogger(testLogger)

	namedLogger := WithName("worker")
	namedLogger.Info("named message")

	// WithName adds the name as a message prefix.
	if testSink.lastMsg != "worker: named message" {
		t.Errorf("expected 'worker: named message', got '%s'", testSink.lastMsg)
	}
}

func TestWithValues(t *testing.T) {
	// Reset Log to its initial state.
	Log = logr.New(NewDelegatingLogSink())

	testSink := &testLogSink{}
	testLogger := logr.New(testSink)
	SetLogger(testLogger)

	valuedLogger := WithValues("component", "worker", "id", "123")
	valuedLogger.Info("valued message")

	if testSink.lastMsg != "valued message" {
		t.Errorf("expected 'valued message', got '%s'", testSink.lastMsg)
	}
	// The key-value pairs added by WithValues are automatically appended to every log.
	if len(testSink.lastKeysAndValues) < 4 {
		t.Errorf("expected at least 4 keysAndValues, got %d", len(testSink.lastKeysAndValues))
	}
}
